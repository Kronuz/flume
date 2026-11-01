// flume_test -- the standalone proof that a file streamed through flume comes out the other
// end byte-identical: round-trip a spread of inputs (empty, one byte, small, multi-block,
// incompressible random) through Sender -> a channel string -> Receiver, feeding the channel
// in several fragment sizes so the partial-varint / split-block paths are exercised. Plus a
// corruption test: a flipped byte must be caught, never silently accepted.

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <random>
#include <string>
#include <unistd.h>
#include <vector>

#include "flume.h"

static int g_fail = 0;
static void check(bool ok, const std::string& m) {
	std::printf("  %s %s\n", ok ? "ok:  " : "FAIL:", m.c_str());
	if (!ok) { ++g_fail; }
}

struct StringWriter {
	std::string& out;
	bool write(std::string_view v) { out.append(v); return true; }
};

struct StringSink {
	std::string& out;
	bool write(const char* d, std::size_t n) { out.append(d, n); return true; }
};

// A temp fd holding `content`, name unlinked (the fd stays open), rewound to the start.
static int temp_fd_with(const std::string& content) {
	char tmpl[] = "/tmp/flume_test_XXXXXX";
	int fd = ::mkstemp(tmpl);
	if (fd == -1) { return -1; }
	if (!content.empty()) {
		[[maybe_unused]] ssize_t w = ::write(fd, content.data(), content.size());
	}
	::unlink(tmpl);
	::lseek(fd, 0, SEEK_SET);
	return fd;
}

// Send `input` through a flume, return the framed channel bytes.
static std::string send_all(const std::string& input, std::size_t block_size) {
	std::string channel;
	StringWriter w{channel};
	int fd = temp_fd_with(input);
	flume::Sender<StringWriter> sender(w, fd, block_size);
	bool ok = sender.send();
	::close(fd);
	if (!ok) { return std::string("\x01SENDFAIL", 9); }  // an unlikely marker
	return channel;
}

// Feed `channel` to a Receiver in `feed_chunk`-sized pieces; return (done, output). A non-zero
// max_block gives the Receiver that cap.
static std::pair<bool, std::string> receive_all(const std::string& channel, std::size_t feed_chunk, std::size_t max_block = 0) {
	std::string output;
	StringSink sink{output};
	flume::Receiver<StringSink> receiver(sink, max_block);
	using Status = flume::Receiver<StringSink>::Status;
	Status st = Status::NeedMore;
	for (std::size_t i = 0; i < channel.size(); i += feed_chunk) {
		std::size_t n = std::min(feed_chunk, channel.size() - i);
		st = receiver.feed(channel.data() + i, n);
		if (st == Status::Done || st == Status::Error) { break; }
	}
	return {st == Status::Done, output};
}

static bool roundtrip(const std::string& input, std::size_t block_size, std::size_t feed_chunk, std::size_t max_block = 0) {
	std::string channel = send_all(input, block_size);
	auto [done, output] = receive_all(channel, feed_chunk, max_block);
	return done && output == input;
}

// A Sink that only counts, for inputs too large to keep.
struct CountSink {
	std::size_t bytes = 0;
	bool write(const char*, std::size_t n) { bytes += n; return true; }
};

int main() {
	std::printf("== flume: round-trip (byte-identical through the channel) ==\n");

	std::mt19937 rng(12345);
	std::string incompressible;
	incompressible.resize(700 * 1024);
	for (auto& c : incompressible) { c = static_cast<char>(rng() & 0xff); }

	std::string big_text;
	big_text.reserve(900 * 1024);
	while (big_text.size() < 900 * 1024) { big_text += "the quick brown fox jumps over the lazy dog 0123456789\n"; }

	std::vector<std::pair<std::string, std::string>> inputs = {
		{"empty", ""},
		{"one byte", "x"},
		{"short text", "hello, flume"},
		{"exactly one block", std::string(256 * 1024, 'A')},
		{"multi-block text (~900KB)", big_text},
		{"incompressible random (~700KB)", incompressible},
	};

	// Several block sizes and feed-fragment sizes: 1 (byte-at-a-time), 7 (odd, splits varints
	// and blocks), and a big chunk. The default block size plus a tiny one to force many blocks.
	std::vector<std::size_t> block_sizes = {flume::DEFAULT_BLOCK_SIZE, 4096};
	std::vector<std::size_t> feed_chunks = {1, 7, 64 * 1024};

	for (const auto& [name, input] : inputs) {
		bool all = true;
		for (auto bs : block_sizes) {
			for (auto fc : feed_chunks) {
				if (!roundtrip(input, bs, fc)) { all = false; }
			}
		}
		check(all, name + " round-trips for every block/feed size");
	}

	std::printf("== flume: corruption is caught ==\n");
	// Flip a byte inside a compressed block; the receiver must NOT report a clean round-trip
	// (either a decompress error or a checksum mismatch).
	{
		std::string channel = send_all(big_text, 4096);
		// pick a byte well past the first block's length prefix
		std::size_t pos = channel.size() / 2;
		channel[pos] = static_cast<char>(channel[pos] ^ 0xff);
		auto [done, output] = receive_all(channel, 64 * 1024);
		check(!(done && output == big_text), "a flipped block byte is rejected (no silent corruption)");
	}
	// Flip the checksum footer's last byte -> checksum mismatch.
	{
		std::string channel = send_all("some payload to checksum", flume::DEFAULT_BLOCK_SIZE);
		channel.back() = static_cast<char>(channel.back() ^ 0x01);
		auto [done, output] = receive_all(channel, 3);
		check(!done, "a flipped checksum footer is rejected");
	}

	std::printf("== flume: leftover (over-read past one transfer) is recoverable ==\n");
	// A multiplexed channel: one transfer immediately followed by other bytes. Feeding the
	// whole thing must Done on the transfer AND hand back the trailing bytes via leftover(),
	// so the caller's own framing can resume where flume stopped.
	{
		std::string channel = send_all("first transfer payload", flume::DEFAULT_BLOCK_SIZE);
		std::string trailer = "\x05NEXT-MESSAGE-BYTES";
		std::string output;
		StringSink sink{output};
		flume::Receiver<StringSink> receiver(sink);
		std::string all = channel + trailer;
		auto st = receiver.feed(all.data(), all.size());
		check(st == flume::Receiver<StringSink>::Status::Done, "transfer completes despite trailing bytes");
		check(output == "first transfer payload", "the transfer round-trips");
		check(receiver.leftover() == trailer, "leftover() returns exactly the trailing bytes");
	}

	std::printf("== flume: the opt-in block cap (Receiver's max_block) ==\n");
	{
		using CountReceiver = flume::Receiver<CountSink>;
		using Status = CountReceiver::Status;
		const std::size_t cap = flume::max_block_for(flume::DEFAULT_BLOCK_SIZE);
		const std::string huge_claim = flume::serialise_length(1ULL << 40);  // a 1 TiB block
		const std::string junk(4 << 20, 'x');

		// Without a cap the Receiver waits for the declared block and buffers every byte behind it.
		{
			CountSink sink;
			CountReceiver r(sink);
			check(r.max_block() == 0, "default: no cap");
			check(r.feed(huge_claim.data(), huge_claim.size()) == Status::NeedMore, "default: a 1 TiB declared block is awaited");
			check(r.feed(junk.data(), junk.size()) == Status::NeedMore, "default: 4 MiB of junk behind it is buffered");
			check(r.leftover().size() == huge_claim.size() + junk.size(), "default: every byte is held");
		}
		// With a cap the length alone is refused: Error at once, nothing buffered, nothing later accepted.
		{
			CountSink sink;
			CountReceiver r(sink, cap);
			check(r.max_block() == cap, "max_block() reports the cap");
			check(r.feed(huge_claim.data(), huge_claim.size()) == Status::Error, "capped: a 1 TiB declared block is refused at once");
			check(r.error() && !r.done(), "capped: error() set");
			check(r.leftover().empty(), "capped: the buffer is released");
			check(r.feed(junk.data(), junk.size()) == Status::Error, "capped: later feeds stay Error");
			check(r.leftover().empty() && sink.bytes == 0, "capped: nothing buffered, nothing written");
		}
		// The boundary: a declared length equal to the cap is awaited, one more is refused.
		{
			CountSink sink;
			CountReceiver at(sink, 1000), over(sink, 1000);
			const std::string at_cap = flume::serialise_length(1000), over_cap = flume::serialise_length(1001);
			check(at.feed(at_cap.data(), at_cap.size()) == Status::NeedMore, "capped: a block of exactly max_block is awaited");
			check(over.feed(over_cap.data(), over_cap.size()) == Status::Error, "capped: a block of max_block + 1 is refused");
		}
		// A length prefix that can never complete (0xff and eleven continuation bytes with no stop bit) is
		// waited for without a cap, so the buffer grows, and refused with one once it is eleven bytes long.
		{
			const std::string bad_prefix = std::string("\xff", 1) + std::string(11, '\0');
			CountSink sink;
			CountReceiver r(sink);
			check(r.feed(bad_prefix.data(), bad_prefix.size()) == Status::NeedMore, "default: an unparsable prefix is awaited");
			check(r.feed(junk.data(), junk.size()) == Status::NeedMore && r.leftover().size() == bad_prefix.size() + junk.size(), "default: and everything behind it is buffered");

			CountSink sink2;
			CountReceiver capped(sink2, cap);
			check(capped.feed(bad_prefix.data(), 10) == Status::NeedMore, "capped: ten bytes of a prefix may still complete");
			check(capped.feed(bad_prefix.data() + 10, 1) == Status::Error, "capped: eleven bytes that do not parse are refused");
			check(capped.leftover().empty(), "capped: the buffer is released after an unparsable prefix");
		}
		// A small frame that declares a huge decompressed size is refused before it is decompressed.
		{
			const std::string zeros(64 << 20, '\0');
			const std::string block = flume::ZstdCodec<>::compress(zeros);
			check(block.size() < 64 * 1024, "64 MiB of zeros is a small frame");
			const std::string frame = flume::serialise_length(block.size()) + block;

			CountSink open_sink;
			CountReceiver open_receiver(open_sink);
			check(open_receiver.feed(frame.data(), frame.size()) == Status::NeedMore && open_sink.bytes == zeros.size(), "default: the frame decompresses to 64 MiB");

			CountSink capped_sink;
			CountReceiver capped(capped_sink, cap);
			check(capped.feed(frame.data(), frame.size()) == Status::Error, "capped: the 64 MiB frame is refused");
			check(capped_sink.bytes == 0, "capped: and nothing reaches the Sink");
		}
		// The same through a Sender: 4096-byte blocks of zeros compress to far less than the cap but
		// decode to more than a cap of 2048.
		{
			const std::string channel = send_all(std::string(64 * 1024, '\0'), 4096);
			CountSink sink;
			CountReceiver r(sink, 2048);
			check(r.feed(channel.data(), channel.size()) == Status::Error && sink.bytes == 0, "capped: a block that decodes past the cap is refused");
		}
		// A cap sized by max_block_for lets every transfer through, whatever the block and feed sizes,
		// including incompressible blocks that grow and blocks of zeros that shrink to a few bytes.
		{
			const std::string zeros(1 << 20, '\0');
			std::mt19937 rng2(777);
			std::string noise(300 * 1024, '\0');
			for (auto& c : noise) { c = static_cast<char>(rng2() & 0xff); }
			bool all = true;
			const std::string* inputs[] = {&noise, &zeros, &big_text, &incompressible};
			for (const std::string* input : inputs) {
				for (auto bs : block_sizes) {
					for (auto fc : feed_chunks) {
						if (!roundtrip(*input, bs, fc, flume::max_block_for(bs))) { all = false; }
					}
				}
			}
			check(all, "a cap of max_block_for(block size) round-trips every input at every feed size");
			check(!roundtrip(big_text, flume::DEFAULT_BLOCK_SIZE, 64 * 1024, 1000), "a cap below the block size refuses the transfer");
		}
		// leftover() still returns the bytes past the footer with a cap.
		{
			std::string channel = send_all("first transfer payload", flume::DEFAULT_BLOCK_SIZE);
			const std::string trailer = "\x05NEXT-MESSAGE-BYTES";
			std::string output;
			StringSink sink{output};
			flume::Receiver<StringSink> receiver(sink, cap);
			const std::string all = channel + trailer;
			check(receiver.feed(all.data(), all.size()) == flume::Receiver<StringSink>::Status::Done && output == "first transfer payload", "capped: the transfer completes despite trailing bytes");
			check(receiver.leftover() == trailer, "capped: leftover() returns exactly the trailing bytes");
		}
	}

	std::printf("\n%s (%d failures)\n", g_fail == 0 ? "PASS" : "FAIL", g_fail);
	return g_fail == 0 ? 0 : 1;
}

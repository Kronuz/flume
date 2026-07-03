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

// Feed `channel` to a Receiver in `feed_chunk`-sized pieces; return (done, output).
static std::pair<bool, std::string> receive_all(const std::string& channel, std::size_t feed_chunk) {
	std::string output;
	StringSink sink{output};
	flume::Receiver<StringSink> receiver(sink);
	using Status = flume::Receiver<StringSink>::Status;
	Status st = Status::NeedMore;
	for (std::size_t i = 0; i < channel.size(); i += feed_chunk) {
		std::size_t n = std::min(feed_chunk, channel.size() - i);
		st = receiver.feed(channel.data() + i, n);
		if (st == Status::Done || st == Status::Error) { break; }
	}
	return {st == Status::Done, output};
}

static bool roundtrip(const std::string& input, std::size_t block_size, std::size_t feed_chunk) {
	std::string channel = send_all(input, block_size);
	auto [done, output] = receive_all(channel, feed_chunk);
	return done && output == input;
}

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

	std::printf("\n%s (%d failures)\n", g_fail == 0 ? "PASS" : "FAIL", g_fail);
	return g_fail == 0 ? 0 : 1;
}

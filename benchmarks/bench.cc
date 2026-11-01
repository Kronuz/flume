// flume bench -- answer "is Zstd worth it over the classic LZ4?" on flume's actual path:
// compress a replication-representative payload in flume's per-block chunks, measuring the
// ratio (bytes on the wire) and compress/decompress throughput for each codec policy. For a
// whole-database transfer the ratio is bandwidth, so a better ratio at competitive speed is
// the win that matters.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <string_view>
#include <vector>

#include <fcntl.h>
#include <unistd.h>

#include "compressor_zstd.h"
#include "compressor_lz4.h"
#include "compressor_deflate.h"
#include "flume.h"

struct Lz4Codec {
	static std::string compress(std::string_view in) { return compress_lz4(in); }
	static std::string decompress(std::string_view in) { return decompress_lz4(in); }
};
struct DeflateCodec {
	static std::string compress(std::string_view in) { return compress_deflate(in); }
	static std::string decompress(std::string_view in) { return decompress_deflate(in); }
};

using clk = std::chrono::steady_clock;
static double ms(clk::duration d) { return std::chrono::duration<double, std::milli>(d).count(); }

// A replication-representative corpus: many small structured "documents" (what Xapiand
// actually indexes + replicates), compressible but not trivially so.
static std::string make_corpus(std::size_t target) {
	std::string s;
	s.reserve(target);
	std::uint64_t seed = 0x9e3779b97f4a7c15ULL;
	std::size_t i = 0;
	const char* cities[] = {"Xanadu", "Cinnabar", "Kronuz", "Dubalu", "Aventine", "Meridian"};
	while (s.size() < target) {
		seed = seed * 6364136223846793005ULL + 1442695040888963407ULL;
		auto r = static_cast<unsigned>(seed >> 33);
		s += "{\"_id\":";
		s += std::to_string(i++);
		s += ",\"user\":\"user_";
		s += std::to_string(r % 100000);
		s += "\",\"city\":\"";
		s += cities[r % 6];
		s += "\",\"score\":";
		s += std::to_string(r % 1000);
		s += ".";
		s += std::to_string(r % 100);
		s += ",\"active\":";
		s += ((r & 1) != 0) ? "true" : "false";
		s += ",\"tags\":[\"alpha\",\"beta\",\"gamma_";
		s += std::to_string(r % 32);
		s += "\"],\"note\":\"the quick brown fox jumps over the lazy dog\"}\n";
	}
	s.resize(target);
	return s;
}

// Every codec must give the corpus back byte for byte; anything else aborts the run.
[[noreturn]] static void fail(const char* name, const char* what) {
	std::fprintf(stderr, "flume_bench: %s: %s\n", name, what);
	std::exit(1);
}

static constexpr int REPS = 3;

// The flume framing end to end: Sender -> an in-memory channel -> Receiver, on one thread. The
// Writer hands every channel write straight to the Receiver, which appends the rebuilt bytes to `out`.
struct OutSink {
	std::string& out;
	bool write(const char* d, std::size_t n) { out.append(d, n); return true; }
};
using Rx = flume::Receiver<OutSink>;
struct MemChannel {
	Rx& rx;
	std::uint64_t wire = 0;
	bool write(std::string_view v) {
		wire += v.size();
		return rx.feed(v.data(), v.size()) != Rx::Status::Error;
	}
};

// Best round trip over REPS runs, in MiB/s of corpus, for a Sender -> channel -> Receiver pass.
static double run_framed(const std::string& corpus, const char* name) {
	char path[] = "flume_bench_XXXXXX";
	int fd = ::mkstemp(path);
	if (fd < 0) { fail(name, "cannot create the corpus file"); }
	::unlink(path);
	for (std::size_t off = 0; off < corpus.size();) {
		ssize_t w = ::write(fd, corpus.data() + off, corpus.size() - off);
		if (w <= 0) { fail(name, "cannot write the corpus file"); }
		off += static_cast<std::size_t>(w);
	}
	double best = 1e300;
	std::uint64_t wire = 0;
	for (int rep = 0; rep < REPS; ++rep) {
		::lseek(fd, 0, SEEK_SET);
		std::string out;
		out.reserve(corpus.size());
		OutSink sink{out};
		Rx rx(sink);
		MemChannel ch{rx};
		flume::Sender<MemChannel> sender(ch, fd);
		auto t0 = clk::now();
		bool ok = sender.send();
		auto t1 = clk::now();
		if (!ok || !rx.done()) { fail(name, "the transfer did not complete"); }
		if (out != corpus) { fail(name, "round trip differs from the corpus"); }
		best = std::min(best, ms(t1 - t0));
		wire = ch.wire;
	}
	::close(fd);
	double mb = static_cast<double>(corpus.size()) / (1024.0 * 1024.0);
	std::printf("  %-22s  ratio %5.2fx   round trip %6.0f MiB/s   (%zu -> %llu bytes)\n", name,
		static_cast<double>(corpus.size()) / static_cast<double>(wire), mb / (best / 1000.0), corpus.size(),
		static_cast<unsigned long long>(wire));
	return mb / (best / 1000.0);
}

static std::vector<std::string_view> split(const std::string& corpus, std::size_t block_size) {
	std::vector<std::string_view> blocks;
	for (std::size_t off = 0; off < corpus.size(); off += block_size) {
		blocks.emplace_back(corpus.data() + off, std::min(block_size, corpus.size() - off));
	}
	return blocks;
}

// The same blocks through the bare codec, no framing and no XXH32: compress each, decompress each, append to one
// output the size of the corpus as the framed pass does. Best of REPS, in MiB/s of corpus.
template <typename Codec>
static void run_bare(const std::string& corpus, std::size_t block_size, const char* name) {
	auto blocks = split(corpus, block_size);
	double best = 1e300;
	for (int rep = 0; rep < REPS; ++rep) {
		std::string out;
		out.reserve(corpus.size());
		auto t0 = clk::now();
		for (auto b : blocks) {
			std::string comp = Codec::compress(b);
			out.append(Codec::decompress(comp));
		}
		auto t1 = clk::now();
		if (out != corpus) { fail(name, "round trip differs from the corpus"); }
		best = std::min(best, ms(t1 - t0));
	}
	double mb = static_cast<double>(corpus.size()) / (1024.0 * 1024.0);
	std::printf("  %-22s  round trip %6.0f MiB/s\n", name, mb / (best / 1000.0));
}

// Best of REPS runs for each figure, in MiB/s of corpus. Decompression frees each block's output as soon as it is
// made, as the Receiver does; a separate untimed pass checks every block against the corpus.
template <typename Codec>
static void run(const std::string& corpus, std::size_t block_size, const char* name) {
	auto blocks = split(corpus, block_size);

	double best_c = 1e300, best_d = 1e300;
	std::size_t total_comp = 0;
	std::vector<std::string> comp;
	for (int rep = 0; rep < REPS; ++rep) {
		comp.clear();
		comp.reserve(blocks.size());
		auto t0 = clk::now();
		total_comp = 0;
		for (auto b : blocks) { comp.push_back(Codec::compress(b)); total_comp += comp.back().size(); }
		auto t1 = clk::now();
		std::size_t total_plain = 0;
		for (auto& c : comp) { total_plain += Codec::decompress(c).size(); }
		auto t2 = clk::now();
		if (total_plain != corpus.size()) { fail(name, "decompressed size differs from the corpus"); }
		best_c = std::min(best_c, ms(t1 - t0));
		best_d = std::min(best_d, ms(t2 - t1));
	}
	for (std::size_t i = 0; i < blocks.size(); ++i) {
		if (Codec::decompress(comp[i]) != blocks[i]) { fail(name, "round trip differs from the corpus"); }
	}

	double mb = static_cast<double>(corpus.size()) / (1024.0 * 1024.0);
	double ratio = static_cast<double>(corpus.size()) / static_cast<double>(total_comp);
	std::printf("  %-22s  ratio %5.2fx   comp %6.0f MiB/s   decomp %6.0f MiB/s   (%zu -> %zu bytes)\n",
		name, ratio, mb / (best_c / 1000.0), mb / (best_d / 1000.0), corpus.size(), total_comp);
}

int main() {
	std::size_t size = 32 * 1024 * 1024;
	std::string corpus = make_corpus(size);
	std::printf("zstd %s, lz4 %s, zlib %s\n", ZSTD_versionString(), LZ4_versionString(), zlibVersion());
	std::printf("== flume codec comparison: %zu MiB replication-like corpus, %zu KB blocks, best of %d ==\n",
		size / (1024 * 1024), flume::DEFAULT_BLOCK_SIZE / 1024, REPS);
	run<Lz4Codec>(corpus, flume::DEFAULT_BLOCK_SIZE, "lz4 (classic)");
	run<DeflateCodec>(corpus, flume::DEFAULT_BLOCK_SIZE, "deflate/zlib");
	run<flume::ZstdCodec<1>>(corpus, flume::DEFAULT_BLOCK_SIZE, "zstd L1 (fast)");
	run<flume::ZstdCodec<3>>(corpus, flume::DEFAULT_BLOCK_SIZE, "zstd L3 (zstd default)");
	run<flume::ZstdCodec<6>>(corpus, flume::DEFAULT_BLOCK_SIZE, "zstd L6 (flume default)");
	run<flume::ZstdCodec<9>>(corpus, flume::DEFAULT_BLOCK_SIZE, "zstd L9");
	std::printf("\n== the framing: zstd L6 round trip without and with it ==\n");
	run_bare<flume::ZstdCodec<6>>(corpus, flume::DEFAULT_BLOCK_SIZE, "zstd L6, no framing");
	run_framed(corpus, "flume (zstd L6)");
	std::printf("\nEvery codec and the framed transfer rebuilt the corpus byte for byte.\n");
	std::printf("ratio = original/compressed (higher = fewer bytes on the wire).\n");
	std::printf("flume defaults Zstd to L6: the ratio knee; see docs/BENCHMARKS.md for its cost.\n");
	return 0;
}

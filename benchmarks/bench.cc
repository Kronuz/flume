// flume bench -- answer "is Zstd worth it over the classic LZ4?" on flume's actual path:
// compress a replication-representative payload in flume's per-block chunks, measuring the
// ratio (bytes on the wire) and compress/decompress throughput for each codec policy. For a
// whole-database transfer the ratio is bandwidth, so a better ratio at competitive speed is
// the win that matters.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <string>
#include <string_view>
#include <vector>

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

template <typename Codec>
static void run(const std::string& corpus, std::size_t block_size, const char* name) {
	std::vector<std::string_view> blocks;
	for (std::size_t off = 0; off < corpus.size(); off += block_size) {
		blocks.emplace_back(corpus.data() + off, std::min(block_size, corpus.size() - off));
	}

	std::vector<std::string> comp;
	comp.reserve(blocks.size());
	auto t0 = clk::now();
	std::size_t total_comp = 0;
	for (auto b : blocks) { comp.push_back(Codec::compress(b)); total_comp += comp.back().size(); }
	auto t1 = clk::now();
	std::size_t total_plain = 0;
	for (auto& c : comp) { total_plain += Codec::decompress(c).size(); }
	auto t2 = clk::now();

	double mb = static_cast<double>(corpus.size()) / (1024.0 * 1024.0);
	double ratio = static_cast<double>(corpus.size()) / static_cast<double>(total_comp);
	double cmbps = mb / (ms(t1 - t0) / 1000.0);
	double dmbps = mb / (ms(t2 - t1) / 1000.0);
	std::printf("  %-22s  ratio %5.2fx   comp %6.0f MB/s   decomp %6.0f MB/s   (%zu -> %zu bytes)\n",
		name, ratio, cmbps, dmbps, corpus.size(), total_comp);
	if (total_plain != corpus.size()) { std::printf("    !! decompress size mismatch\n"); }
}

int main() {
	std::size_t size = 32 * 1024 * 1024;
	std::string corpus = make_corpus(size);
	std::printf("== flume codec comparison: %zu MB replication-like corpus, %zu KB blocks ==\n",
		size / (1024 * 1024), flume::DEFAULT_BLOCK_SIZE / 1024);
	run<Lz4Codec>(corpus, flume::DEFAULT_BLOCK_SIZE, "lz4 (classic)");
	run<DeflateCodec>(corpus, flume::DEFAULT_BLOCK_SIZE, "deflate/zlib");
	run<flume::ZstdCodec<1>>(corpus, flume::DEFAULT_BLOCK_SIZE, "zstd L1 (fast)");
	run<flume::ZstdCodec<3>>(corpus, flume::DEFAULT_BLOCK_SIZE, "zstd L3 (zstd default)");
	run<flume::ZstdCodec<6>>(corpus, flume::DEFAULT_BLOCK_SIZE, "zstd L6 (flume default)");
	run<flume::ZstdCodec<9>>(corpus, flume::DEFAULT_BLOCK_SIZE, "zstd L9");
	std::printf("\nratio = original/compressed (higher = fewer bytes on the wire).\n");
	std::printf("flume defaults Zstd to L6: the ratio knee, still faster than any normal link.\n");
	return 0;
}

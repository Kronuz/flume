/*
 * Copyright (c) 2026 Germán Méndez Bravo (Kronuz)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

// flume -- carry a file (or any fd-backed byte source) downstream over a byte channel as
// compressed, framed, integrity-checked blocks, in bounded memory. A flume is a channel
// that floats logs downstream: you feed a file in one end and it comes out the other,
// smaller and intact, without ever holding the whole thing in memory.
//
// It is the transport-layer counterpart to the Kronuz/compressors BUFFER-CORE codecs:
// compressors compresses in-memory buffers; flume owns the fd reading, the on-wire framing,
// and the end-to-end integrity check, and delegates the per-block squeeze to a codec policy
// (Zstd by default -- a better ratio than the classic LZ4, so fewer bytes cross the wire).
//
// The frame (one transfer) is a run of length-prefixed compressed blocks, a zero-length
// terminator, and an XXH32 footer over the *uncompressed* bytes:
//
//     [len_1][block_1] [len_2][block_2] ... [0] [xxh32]
//
// where each block_i = codec::compress(read_chunk_i), len is serialise_length(block.size()),
// and the reader verifies the running XXH32 against the footer. This is the modern
// replacement for the LZ4 ClientLZ4Compressor/ClientLZ4Decompressor connection codec.
//
// Channel-agnostic by design:
//   * Sender<Writer>  drives an fd -> a Writer sink   (Writer::write(std::string_view) -> bool).
//   * Receiver<Sink>  is fed channel bytes as they arrive -> a Sink (Sink::write(const char*,
//     std::size_t) -> bool), so a partial/streamed read Just Works.
// The Writer is where the transport plugs in (append to a reply buffer, async_write a socket,
// enqueue a libev buffer -- flume does not care). Header-only, C++20.

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <unistd.h>          // ::read

#include "compressor_zstd.h" // compress_zstd / decompress_zstd (Kronuz/compressors)
#include "xxh32_stream.h"    // compressors::XXH32_* (Kronuz/compressors, vendored XXH32)

namespace flume {

// The fd is read in chunks of this size; each chunk becomes one compressed block. Large
// enough that the codec has real context to work with and the per-block framing overhead is
// negligible, small enough that memory stays bounded regardless of file size.
constexpr std::size_t DEFAULT_BLOCK_SIZE = 256 * 1024;

// The XXH32 seed. Matches the classic connection codec's COMPRESSION_SEED so the digest
// space lines up with the format flume replaces.
constexpr std::uint32_t CHECKSUM_SEED = 0xCEED;


// ---- length codec (vendored, byte-compatible with Kronuz serialise_length) --------------
// A byte < 255 is the length verbatim; 0xff introduces a (len - 255) little-endian base-128
// continuation whose final byte has 0x80 set. unserialise returns false on short/bad input,
// which the incremental Receiver reads as "need more bytes".

inline std::string serialise_length(unsigned long long len) {
	std::string result;
	if (len < 255) {
		result += static_cast<char>(static_cast<unsigned char>(len));
	} else {
		result += '\xff';
		len -= 255;
		while (true) {
			auto b = static_cast<unsigned char>(len & 0x7f);
			len >>= 7;
			if (len == 0) {
				result += static_cast<char>(b | static_cast<unsigned char>(0x80));
				break;
			}
			result += static_cast<char>(b);
		}
	}
	return result;
}

inline bool unserialise_length(const char** p, const char* end, unsigned long long& out) {
	if (*p == end) { return false; }
	unsigned long long len = static_cast<unsigned char>(*(*p)++);
	if (len == 0xff) {
		len = 0;
		unsigned char ch = 0;
		unsigned shift = 0;
		do {
			if (*p == end || shift > (sizeof(unsigned long long) * 8 / 7 * 7)) { return false; }
			ch = static_cast<unsigned char>(*(*p)++);
			len |= static_cast<unsigned long long>(ch & 0x7f) << shift;
			shift += 7;
		} while ((ch & 0x80) == 0);
		len += 255;
	}
	out = len;
	return true;
}


// ---- codec policies (compile-time seam; default Zstd) -----------------------------------
// A codec is just: compress(view) -> string and decompress(view) -> string, both whole-block
// (the compressors buffer-core). Swap in an LZ4/Deflate policy for interop or a ratio/speed
// trade without touching the framing.

// flume defaults Zstd to level 6, above zstd's own default of 3: flume carries BULK files
// over a channel where the ratio IS bandwidth, and 3 -> 6 buys ~27% fewer bytes for ~3x the
// compress CPU (still ~350 MB/s here, faster than any normal link) -- worth it on a transfer
// path, measured in benchmarks/bench.cc. Crucially the LEVEL is NOT part of the wire format:
// a Zstd frame is self-describing, so any level decodes with any decoder (only the CODEC
// choice is a wire-format decision). Override per call site -- ZstdCodec<1> for a fast/tiny
// transfer, ZstdCodec<19> to wring out a bandwidth-starved link.
constexpr int DEFAULT_ZSTD_LEVEL = 6;

template <int Level = DEFAULT_ZSTD_LEVEL>
struct ZstdCodec {
	static std::string compress(std::string_view in) {
		std::string out;
		::ZstdCompressData compressor(in.data(), in.size(), Level);
		for (auto it = compressor.begin(); it; ++it) { out.append(*it); }
		return out;
	}
	static std::string decompress(std::string_view in) { return decompress_zstd(in); }
};

// Other codecs are one-liner policies too: define an Lz4Codec / DeflateCodec (compress_lz4 /
// compress_deflate from Kronuz/compressors) after including that codec's header, and pass it
// as the second template argument to Sender/Receiver. flume.h stays Zstd-only so a Zstd-only
// consumer needs neither lz4 nor zlib on the include path.


// ---- Sender: fd -> framed compressed blocks -> Writer -----------------------------------
// Writer::write(std::string_view) -> bool  (false aborts the transfer). send() streams the fd
// in bounded memory: one read chunk -> one compressed block -> framed to the Writer, then the
// zero terminator + the XXH32 footer over the uncompressed bytes. Returns false on any read or
// write failure, true on a complete transfer.
template <typename Writer, typename Codec = ZstdCodec<>>
class Sender {
	Writer& writer_;
	int fd_;
	std::size_t block_size_;

public:
	Sender(Writer& writer, int fd, std::size_t block_size = DEFAULT_BLOCK_SIZE)
		: writer_(writer), fd_(fd), block_size_(block_size != 0 ? block_size : DEFAULT_BLOCK_SIZE) {}

	bool send() {
		compressors::XXH32_state_t st;
		compressors::XXH32_reset(&st, CHECKSUM_SEED);

		std::string chunk;
		chunk.resize(block_size_);
		for (;;) {
			ssize_t n = ::read(fd_, chunk.data(), block_size_);
			if (n < 0) { return false; }
			if (n == 0) { break; }
			compressors::XXH32_update(&st, chunk.data(), static_cast<std::size_t>(n));
			std::string comp = Codec::compress(std::string_view(chunk.data(), static_cast<std::size_t>(n)));
			if (!writer_.write(serialise_length(comp.size()))) { return false; }
			if (!writer_.write(std::string_view(comp))) { return false; }
		}
		if (!writer_.write(serialise_length(0))) { return false; }
		if (!writer_.write(serialise_length(compressors::XXH32_digest(&st)))) { return false; }
		return true;
	}
};


// ---- Receiver: incremental channel bytes -> Sink, checksum-verified ---------------------
// Sink::write(const char*, std::size_t) -> bool  (false aborts). feed() is called repeatedly
// as channel bytes arrive; it buffers only what it cannot yet parse, decompresses each
// complete block to the Sink, and on the terminator verifies the running XXH32 against the
// footer. A block that straddles two feeds, or a varint split across feeds, is handled.
template <typename Sink, typename Codec = ZstdCodec<>>
class Receiver {
public:
	enum class Status { NeedMore, Done, Error };

	explicit Receiver(Sink& sink) : sink_(sink) {
		compressors::XXH32_reset(&st_, CHECKSUM_SEED);
	}

	Status feed(const char* data, std::size_t n) {
		if (done_) { return Status::Done; }
		if (error_) { return Status::Error; }
		buf_.append(data, n);

		for (;;) {
			const char* base = buf_.data();
			const char* end = base + buf_.size();
			const char* q = base;
			unsigned long long len = 0;
			if (!unserialise_length(&q, end, len)) { return Status::NeedMore; }  // varint incomplete

			if (expect_footer_) {
				buf_.erase(0, static_cast<std::size_t>(q - base));
				if (static_cast<std::uint32_t>(len) != compressors::XXH32_digest(&st_)) {
					error_ = true;
					return Status::Error;
				}
				done_ = true;
				return Status::Done;
			}

			if (len == 0) {                                   // terminator: the footer varint is next
				buf_.erase(0, static_cast<std::size_t>(q - base));
				expect_footer_ = true;
				continue;
			}

			if (static_cast<unsigned long long>(end - q) < len) { return Status::NeedMore; }  // block incomplete

			std::string plain;
			try {
				plain = Codec::decompress(std::string_view(q, static_cast<std::size_t>(len)));
			} catch (...) {
				error_ = true;
				return Status::Error;
			}
			if (!sink_.write(plain.data(), plain.size())) {
				error_ = true;
				return Status::Error;
			}
			compressors::XXH32_update(&st_, plain.data(), plain.size());
			buf_.erase(0, static_cast<std::size_t>(q - base) + static_cast<std::size_t>(len));
		}
	}

	bool done() const { return done_; }
	bool error() const { return error_; }

private:
	Sink& sink_;
	std::string buf_;
	compressors::XXH32_state_t st_;
	bool done_ = false;
	bool error_ = false;
	bool expect_footer_ = false;   // saw the zero-length terminator; the next varint is the digest
};

}  // namespace flume

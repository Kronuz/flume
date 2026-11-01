# flume

Part of [Kronuz C++ Libraries](https://github.com/Kronuz/cpp-libraries), a collection of small, reusable C++ libraries.

[![CI](https://github.com/Kronuz/flume/actions/workflows/ci.yml/badge.svg)](https://github.com/Kronuz/flume/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
![C++20](https://img.shields.io/badge/C%2B%2B-20-blue.svg)
![Header-only](https://img.shields.io/badge/header--only-yes-green.svg)

Header-only file transfer for C++20: stream a file over any byte channel as framed Zstd blocks with an end-to-end checksum.
The Sender reads the file 256 KB at a time, so its memory does not grow with
the file, and the Receiver accepts bytes in whatever pieces the channel
delivers. Reach for it to ship whole files between processes or hosts:
replication, snapshots, backups. A flume is the wooden channel that floats logs
downstream.

![flume: the Sender reads, compresses and frames 256 KB blocks; the Receiver decompresses them to the Sink and checks the XXH32](docs/flume.svg)

## ✨ Highlights

- **Bounded sender memory.** One 256 KB read buffer and one compressed block,
  whatever the file size.
- **Zstd level 6 by default.** On the benchmark's 32 MiB JSON corpus that is a
  15.1x ratio against 7.9x for LZ4, compressing at 357 MiB/s on one M4 Pro core
  and 131 MiB/s on one i9 core. The level is not part of the wire format:
  any `ZstdCodec<N>` output decodes with any Zstd decoder.
- **End-to-end integrity.** An XXH32 over the uncompressed bytes catches a
  flipped byte or a truncation, and a codec that throws on corrupt input becomes
  `Status::Error`.
- **Any transport.** The caller supplies the `Writer` and the `Sink`;
  `Receiver::feed` handles a block or a length split across reads.
- **A small frame.** `[len][block] ... [0][xxh32]`, with varint lengths; a zero
  length ends the transfer.
- **Pluggable codec.** The codec is a template policy with `compress` and
  `decompress`; an LZ4 or deflate policy is three lines over
  [compressors](https://github.com/Kronuz/compressors).
- **Clean hand-off.** After `Done`, `leftover()` holds any bytes fed past the
  footer, for a caller that carries more on the same channel.

## 🚀 Quick start

```cpp
#include "flume.h"

#include <algorithm>
#include <cstdio>
#include <string>
#include <string_view>

struct Channel {  // the Sender's Writer: a socket in real use, a string here
	std::string bytes;
	bool write(std::string_view v) { bytes.append(v); return true; }
};

struct File {  // the Receiver's Sink: the rebuilt file
	std::string data;
	bool write(const char* d, std::size_t n) { data.append(d, n); return true; }
};

int main() {
	std::FILE* f = std::tmpfile();
	for (int i = 0; i < 100000; ++i)
		std::fprintf(f, "GET /index.html 200 %d\n", i % 7);
	std::fflush(f);
	long size = std::ftell(f);
	std::rewind(f);

	Channel channel;
	flume::Sender<Channel> sender(channel, fileno(f));
	if (!sender.send())
		return 1;

	File out;
	flume::Receiver<File> receiver(out);
	using Status = flume::Receiver<File>::Status;
	Status st = Status::NeedMore;
	for (std::size_t at = 0; at < channel.bytes.size() && st == Status::NeedMore; at += 1000) {
		std::size_t n = std::min<std::size_t>(1000, channel.bytes.size() - at);
		st = receiver.feed(channel.bytes.data() + at, n);  // bytes arrive in any pieces
	}
	std::printf("%ld bytes, %zu on the wire, %s\n", size, channel.bytes.size(),
	            st == Status::Done ? "checksum ok" : "corrupt");
	// 2200000 bytes, 758 on the wire, checksum ok (zstd 1.5.7)
}
```

A socket `Writer` loops on short writes;
[`examples/pipe_file.cc`](examples/pipe_file.cc) sends an 8 MB file over a
`socketpair` that way and checks it arrives byte-identical.

## 📦 Installation

Requires a C++20 compiler and CMake 3.16 or newer. It fetches
[compressors](https://github.com/Kronuz/compressors), which needs zlib and uses
the system Zstd and LZ4 when present (fetching them otherwise). CI builds it on
Linux (x86_64, arm64), macOS (arm64, x86_64), and FreeBSD, plus ASan, UBSan, and
TSan builds.

```cmake
include(FetchContent)
FetchContent_Declare(flume
  GIT_REPOSITORY https://github.com/Kronuz/flume.git
  GIT_TAG        3a49d36afecde02d8bce92588930835b020e6267)  # or main for the tip
FetchContent_MakeAvailable(flume)

target_link_libraries(my_app PRIVATE flume::flume)
```

```cpp
#include "flume.h"
```

On macOS with Homebrew, configure with `-DCMAKE_PREFIX_PATH="$(brew --prefix)"`
so CMake finds the compression libraries.

## 🎯 When to use it

Use it to move whole files between processes or hosts over a channel you already
have: a socket, a pipe, or a connection's write queue.

Not for:

- **Several files, retries, or resume.** flume frames one transfer; multiplexing,
  the message that announces a file, and resuming after a drop are the
  caller's.
- **Untrusted senders.** XXH32 detects accidents, not tampering. By default the
  Receiver buffers a block of whatever length the frame declares and
  decompresses it without a size cap, so a sender can make it hold or allocate
  as much as it likes. `Receiver(sink, max_block)` is the opt-in cap: a longer
  declared block, a block that decodes to more, or a length prefix that cannot
  complete is `Status::Error`. Size it with `flume::max_block_for(block_size)`;
  it bounds memory, not who is talking.
- **Acting on data before `Done`.** The Sink receives each block as it
  decompresses; only `Status::Done` says the whole file matched its checksum.
- **Sources without a file descriptor.** The Sender reads with POSIX `::read`.

## ⚡ Performance

`flume_bench` cuts a 32 MiB corpus of replication-like JSON documents into flume's 256 KB
blocks and compresses and decompresses every block with each codec a `Sender` can be given.
The row labels are those codec settings, not competing libraries: LZ4, deflate and Zstd at
four levels are what a `Sender` or `Receiver` is built with, called through
[compressors](https://github.com/Kronuz/compressors) from
[Zstd](https://github.com/facebook/zstd) 1.5.7, [LZ4](https://github.com/lz4/lz4) 1.10.0
(both from Homebrew) and [zlib](https://zlib.net/) 1.2.12 (the macOS SDK's), the same
versions on both machines. Ratio is uncompressed size over compressed size, higher is
better; each speed column is MiB/s of uncompressed data on one core, higher is better; the
bold row is flume's default. Every codec gives the corpus back byte for byte: `flume_bench`
checks every block and aborts on a mismatch. Medians of five runs, each the best of three,
on an Apple M4 Pro (macOS, arm64) and an Intel Core i9-9980HK (macOS 15, x86-64), Apple
clang 17, `-O3 -DNDEBUG`:

| Codec | Ratio | M4 Pro compress | M4 Pro decompress | i9 compress | i9 decompress |
| --- | ---: | ---: | ---: | ---: | ---: |
| LZ4 | 7.90x | 2,235 | 8,238 | 1,333 | 4,541 |
| deflate (zlib) | 13.13x | 219 | 3,445 | 151 | 1,916 |
| Zstd level 1 | 13.21x | 1,683 | 3,392 | 862 | 2,145 |
| Zstd level 3 | 12.63x | 1,392 | 3,430 | 616 | 2,178 |
| **Zstd level 6 (default)** | **15.14x** | **357** | **4,078** | **131** | **2,800** |
| Zstd level 9 | 15.79x | 195 | 4,225 | 80 | 2,969 |

Level 6 puts 2.22 MB on the wire where LZ4 puts 4.25 MB, 48% fewer bytes. Compression, not
decompression, bounds a transfer at level 6.

No other library sends a file as framed, checksummed compressed blocks over an injected
channel, so the framing has no comparator; the bare codec is its bound. The second table
times one `Sender` to `Receiver` pass over an in-memory channel at the default codec (read
the file, compress, frame, feed, decompress, check the XXH32, one thread) against the same
Zstd level 6 applied to the same blocks with no framing and no XXH32. Both are compress plus
decompress end to end, in MiB/s of the corpus on one core, and both rebuild the corpus byte
for byte:

| Zstd level 6 round trip | M4 Pro | i9 |
| --- | ---: | ---: |
| bare codec, no framing, no XXH32 | 328 | 124 |
| flume `Sender` to `Receiver` | 303 | 116 |

The framing and the XXH32 cost 7.6% of the round trip on the M4 Pro and 6.5% on the i9
(within the i9's run-to-run spread, [docs/BENCHMARKS.md](docs/BENCHMARKS.md)), and add 519
bytes to the 2,215,976 of the compressed blocks. The figures above the second table time the
codecs alone. `flume_bench` prints the best of three runs, so the tables take the medians
of five invocations:

```sh
cmake -B build -DCMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH="$(brew --prefix)"
cmake --build build --target flume_bench
./build/flume_bench
```

[docs/BENCHMARKS.md](docs/BENCHMARKS.md) has the method, the spread of the runs and the
choice of level.

## 📚 Documentation

- [ARCHITECTURE.md](ARCHITECTURE.md): the layering, the frame, Sender and
  Receiver, and the codec seam.
- [docs/BENCHMARKS.md](docs/BENCHMARKS.md): codecs and Zstd levels compared on
  the benchmark corpus.
- [AGENTS.md](AGENTS.md): repository map and invariants.
- [CHANGELOG.md](CHANGELOG.md): dated changes.
- [Catalog entry](https://github.com/Kronuz/cpp-libraries/blob/main/AGENTS.md#flume)
  in Kronuz C++ Libraries.

## 🛠️ Building and testing

```sh
cmake -B build
cmake --build build
ctest --test-dir build
./build/flume_bench
```

The tests, `flume_pipe_file`, and `flume_bench` build whenever flume is the
top-level project.

## 🤝 Contributing

Issues and pull requests are welcome. Read [AGENTS.md](AGENTS.md) and the
catalog's [rules of engagement](https://github.com/Kronuz/cpp-libraries/blob/main/AGENTS.md#rules-of-engagement)
first; CI must pass on every platform.

## 📄 License

MIT, Copyright (c) 2026 Germán Méndez Bravo (Kronuz). See [LICENSE](LICENSE).

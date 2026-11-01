# flume codec benchmarks

`benchmarks/bench.cc` cuts a 32 MiB corpus of replication-like JSON documents into flume's 256 KB blocks and
compresses and decompresses every block with each codec a `Sender` can be given, then times the framing. The rows are
codec settings of flume (a `Sender<Writer, Codec>` and its `Receiver`), not competing libraries; the codecs come
from [Zstd](https://github.com/facebook/zstd) 1.5.7 and [LZ4](https://github.com/lz4/lz4) 1.10.0 (Homebrew) and
[zlib](https://zlib.net/) 1.2.12 (the macOS SDK's) through [compressors](https://github.com/Kronuz/compressors).
The bench prints the versions it linked. Both machines run the same versions.

- **Ratio** is uncompressed size over compressed size: higher means fewer bytes on the wire.
- **compress** and **decompress** are MiB/s of uncompressed data on one core: higher is faster. Decompression frees
  each block's output as it is made, as the `Receiver` does.
- **Round trip** is compress plus decompress end to end in MiB/s of the corpus.
- The bench aborts with a message unless every codec, and the framed transfer, rebuild the corpus byte for byte (an
  untimed pass compares every block).

```sh
cmake -B build -DCMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH="$(brew --prefix)"
cmake --build build --target flume_bench
for i in 1 2 3 4 5; do ./build/flume_bench; done
```

One invocation prints the best of three runs of each figure. The tables take the median of five invocations, each
starting a new process. Apple clang 17 on macOS, `-O3 -DNDEBUG`. Apple M4 Pro (arm64), then Intel Core i9-9980HK
(x86_64, macOS 15). Other work on both hosts is the main source of spread:

| Codec | Ratio | M4 Pro compress | M4 Pro decompress | i9 compress | i9 decompress |
| --- | ---: | ---: | ---: | ---: | ---: |
| LZ4 | 7.90x | 2,235 | 8,238 | 1,333 | 4,541 |
| deflate (zlib) | 13.13x | 219 | 3,445 | 151 | 1,916 |
| Zstd level 1 | 13.21x | 1,683 | 3,392 | 862 | 2,145 |
| Zstd level 3 | 12.63x | 1,392 | 3,430 | 616 | 2,178 |
| **Zstd level 6 (flume default)** | **15.14x** | **357** | **4,078** | **131** | **2,800** |
| Zstd level 9 | 15.79x | 195 | 4,225 | 80 | 2,969 |

The lowest and highest of the five invocations, same columns:

| Codec | M4 Pro compress | M4 Pro decompress | i9 compress | i9 decompress |
| --- | ---: | ---: | ---: | ---: |
| LZ4 | 2,227 to 2,259 | 8,096 to 8,627 | 1,275 to 1,481 | 4,179 to 5,391 |
| deflate (zlib) | 213 to 219 | 3,424 to 3,495 | 147 to 160 | 1,721 to 1,980 |
| Zstd level 1 | 1,680 to 1,705 | 3,344 to 3,436 | 811 to 1,027 | 2,133 to 2,442 |
| Zstd level 3 | 1,387 to 1,407 | 3,414 to 3,447 | 595 to 725 | 2,165 to 2,466 |
| Zstd level 6 (flume default) | 355 to 363 | 4,043 to 4,112 | 118 to 137 | 2,568 to 2,958 |
| Zstd level 9 | 193 to 197 | 4,215 to 4,240 | 74 to 82 | 2,939 to 3,138 |

## The framing

No library sends a file as framed, checksummed compressed blocks over an injected channel, so the framing has no
comparator and the bare codec is its bound. The bench times one `Sender` to `Receiver` pass at the default codec over
an in-memory channel: the `Sender` reads the corpus from a file in the page cache, compresses and frames each block,
the channel hands every write to `Receiver::feed`, which decompresses, updates the XXH32 and appends to one output the
size of the corpus; the footer is checked. It also times the same Zstd level 6 on the same blocks with no framing and
no XXH32 (compress each block, decompress it, append it to the same kind of output). Both run on one thread, in MiB/s
of the corpus, the median of five invocations:

| Zstd level 6 round trip | M4 Pro | i9 |
| --- | ---: | ---: |
| bare codec, no framing, no XXH32 | 328 (323 to 329) | 124 (111 to 129) |
| flume `Sender` to `Receiver` | 303 (302 to 309) | 116 (104 to 121) |

The framing and the XXH32 cost 7.6% of the round trip on the M4 Pro and 6.5% on the i9, where the two ranges overlap.
The framed stream is 2,216,495 bytes against 2,215,976 for the blocks alone: 519 bytes of length prefixes and
footer over 128 blocks.

## Why Zstd by default

Zstd gives a much better ratio than LZ4 at lower speed, and for a whole-database transfer the
ratio *is* the bandwidth.

flume defaults Zstd to **level 6**, above zstd's own default of 3: it carries bulk files where
the ratio is bandwidth, and level 6 is the ratio knee, **48% fewer bytes than LZ4** (2.22 MB
against 4.25 MB for 32 MiB). It compresses 357 MiB/s of input on one M4 Pro core and
131 MiB/s on one i9 core, so a link faster than the sender's compression wants a lower level
or several senders. Decompression is not slower at higher levels, since there is less to
decode. Deflate reaches 13.13x against 15.14x; it compresses 1.6 times slower than level 6 on
the M4 Pro and 1.2 times faster on the i9, and decompresses 1.2 to 1.5 times slower on both.

**The level is a knob, not a wire-format decision.** A Zstd frame is self-describing, so any
level decodes with any decoder -- override per call site with `ZstdCodec<1>` (fast/tiny) or
`ZstdCodec<19>` (max ratio for a bandwidth-starved link) freely, both ends need not match. The
*codec* (Zstd vs LZ4) is the wire-format choice; the *level* is not. LZ4/Deflate remain
one-line policies away for interop.

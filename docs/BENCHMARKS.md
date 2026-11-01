# flume codec benchmarks

`benchmarks/bench.cc` compresses a 32 MiB corpus of replication-like JSON documents in
flume's 256 KB blocks with each codec policy and prints the ratio and the compress and
decompress throughput on one core. It prints MiB/s under the label `MB/s`. The figures time
the codecs alone, not the framing or the XXH32.

```sh
cmake -B build -DCMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH="$(brew --prefix)"
cmake --build build --target flume_bench
./build/flume_bench
```

Best of three runs, MiB/s. Apple clang 17 on macOS, `-O3 -DNDEBUG`; Zstd 1.5.7, LZ4 1.10.0 and
zlib 1.2.12 (the macOS SDK's), the same on both machines. Apple M4 Pro (arm64), then Intel Core
i9-9980HK (x86_64):

| Codec | Ratio | M4 Pro compress | M4 Pro decompress | i9 compress | i9 decompress |
| --- | ---: | ---: | ---: | ---: | ---: |
| LZ4 | 7.90x | 2,179 | 8,767 | 1,224 | 4,202 |
| deflate (zlib) | 13.13x | 210 | 2,678 | 155 | 1,401 |
| Zstd level 1 | 13.21x | 1,699 | 3,065 | 939 | 2,146 |
| Zstd level 3 | 12.63x | 1,407 | 3,038 | 679 | 2,183 |
| **Zstd level 6 (flume default)** | **15.14x** | **363** | **3,558** | **128** | **2,137** |
| Zstd level 9 | 15.79x | 198 | 3,724 | 82 | 2,130 |

## Why Zstd by default

Zstd gives a much better ratio than LZ4 at lower speed, and for a whole-database transfer the
ratio *is* the bandwidth.

flume defaults Zstd to **level 6**, above zstd's own default of 3: it carries bulk files where
the ratio is bandwidth, and level 6 is the ratio knee, **48% fewer bytes than LZ4** (2.22 MB
against 4.25 MB for 32 MiB). It compresses 363 MiB/s of input on one M4 Pro core and
128 MiB/s on one i9 core, so a link faster than the sender's compression wants a lower level
or several senders. Decompression is not slower at higher levels, since there is less to
decode. Deflate reaches 13.13x against 15.14x; it compresses 1.7 times slower than level 6 on
the M4 Pro and 1.2 times faster on the i9, and decompresses 1.3 to 1.5 times slower on both.

**The level is a knob, not a wire-format decision.** A Zstd frame is self-describing, so any
level decodes with any decoder -- override per call site with `ZstdCodec<1>` (fast/tiny) or
`ZstdCodec<19>` (max ratio for a bandwidth-starved link) freely, both ends need not match. The
*codec* (Zstd vs LZ4) is the wire-format choice; the *level* is not. LZ4/Deflate remain
one-line policies away for interop.

# flume codec benchmarks

Reproduce with `benchmarks/bench.cc` (`cmake --build build && ./build/flume_bench`).

## Why Zstd by default

The classic codec was LZ4. Zstd ships a much better ratio at competitive speed, and for a
whole-database transfer the ratio *is* the bandwidth. Measured on a 32 MB replication-like
corpus in flume's 256 KB blocks (`benchmarks/bench.cc`):

| codec          | ratio  | compress   | decompress |
| -------------- | ------ | ---------- | ---------- |
| lz4 (classic)  | 7.90x  | 1671 MB/s  | 3346 MB/s  |
| deflate/zlib   | 13.13x | 220 MB/s   | 2701 MB/s  |
| zstd L3        | 12.63x | 1467 MB/s  | 3231 MB/s  |
| **zstd L6 (flume default)** | **15.14x** | **379 MB/s** | **3796 MB/s** |
| zstd L9        | 15.79x | 209 MB/s   | 3928 MB/s  |

flume defaults Zstd to **level 6**, above zstd's own default of 3: it carries bulk files where
the ratio is bandwidth, and L6 is the ratio knee -- **~48% fewer bytes than LZ4** (2.22 MB vs
4.25 MB for 32 MB) at 379 MB/s, which still saturates any normal link on one core (and
decompress is actually *faster* at higher levels, since there is less to decode). Deflate's
ratio is close but at 6.5x the compression time, which a data path cannot spend.

**The level is a knob, not a wire-format decision.** A Zstd frame is self-describing, so any
level decodes with any decoder -- override per call site with `ZstdCodec<1>` (fast/tiny) or
`ZstdCodec<19>` (max ratio for a bandwidth-starved link) freely, both ends need not match. The
*codec* (Zstd vs LZ4) is the wire-format choice; the *level* is not. LZ4/Deflate remain
one-line policies away for interop.

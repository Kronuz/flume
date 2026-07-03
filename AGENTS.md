# AGENTS.md

Working notes for agents modifying this repository. For the design read `ARCHITECTURE.md`;
for usage and the codec comparison read `README.md`.

## What this is

`flume.h` (one header) streams an fd-backed byte source over a channel as compressed, framed,
integrity-checked blocks, in bounded memory. It is the transport-layer half of a file
transfer; the codec half is [Kronuz/compressors](https://github.com/Kronuz/compressors)
(buffer-core). It replaces the LZ4 `ClientLZ4Compressor`/`ClientLZ4Decompressor` connection
codec that lived in the libev networking lib.

## Repo map

```
flume.h                 The whole library: length codec + ZstdCodec + Sender<Writer> + Receiver<Sink>.
test/flume_test.cc      Round-trip (empty..incompressible), partial-feed, corruption detection. CTest `flume`.
examples/pipe_file.cc   Stream a real file through a flume over a socketpair; verify byte-identical.
benchmarks/bench.cc     Zstd vs LZ4 vs Deflate: ratio + throughput on a replication-like corpus.
CMakeLists.txt          Header-only INTERFACE target flume::flume; FetchContents compressors; top-level-only tests.
```

## Build & test

```sh
cmake -B build -DCMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH="$(brew --prefix)"
cmake --build build
ctest --test-dir build
```

The one CTest test round-trips a spread of inputs across block sizes and feed-fragment sizes
and ends `PASS (0 failures)`. `-DFETCHCONTENT_SOURCE_DIR_COMPRESSORS=<path>` builds against a
local compressors checkout. On macOS the keg-only Homebrew zstd/lz4/zlib need the
`CMAKE_PREFIX_PATH`.

## Load-bearing invariants (do not regress)

- **`decompress(send(x)) == x`, byte for byte, for every input** -- empty, one byte, a buffer
  exactly one block, multi-block, and incompressible random. The test pins all of these across
  several block and feed-fragment sizes; extend it for any new input class.
- **The Receiver is a true incremental parser.** `feed()` must handle a varint or a block split
  across calls (return `NeedMore`, buffer the remainder). Feeding one byte at a time must
  round-trip. The test feeds at 1, 7, and 64 KB.
- **Corruption is caught, never silently accepted.** A flipped block byte (codec throws, or the
  running XXH32 mismatches) and a flipped footer both return `Error` / not-`Done`. Keep the
  `try/catch` around `Codec::decompress` and the footer comparison.
- **The frame is `[len][block]...[0][xxh32]`.** `len` is `serialise_length` (the varint), the
  zero-length block terminates, the footer is the XXH32 (seed `0xCEED`) over the *uncompressed*
  bytes. Changing any of this is a wire-format change (see below).
- **Memory stays bounded.** Sender holds one read chunk + one compressed block; Receiver holds
  only unparsed bytes. Do not accumulate the whole file anywhere.

## Codec: Zstd by default, and it is a wire-format decision

The default codec is **Zstd**, not the classic LZ4: `benchmarks/bench.cc` shows ~37% fewer
bytes at competitive speed on a replication-like corpus (12.63x vs 7.90x). For a whole-database
transfer the ratio is bandwidth, so this is an efficiency win, not just latency.

**The codec is part of the wire format.** Both ends of a transfer must use the same `Codec`
template argument. Changing the default, or mixing codecs across a version boundary, is a
**compatibility event**: bump the enclosing protocol version and move both ends together. This
is fine when you own both ends and are pre-production (Xapiand's cluster protocol is
version-gated); flag it loudly otherwise. `flume.h` stays Zstd-only on the include path so a
Zstd consumer does not drag in lz4/zlib; define `Lz4Codec`/`DeflateCodec` locally (three lines
over the compressors helpers) only where you need interop.

## Traps

- **Don't add fd-streaming to compressors.** That library is buffer-core by charter; the fd
  reading belongs here. If you need a new codec, add it to compressors as a buffer-core
  backend, then use it via a one-line flume policy.
- **`compress_zstd`/`decompress_zstd` are global** (no namespace); the XXH32 streaming symbols
  are in `namespace compressors` (`compressors::XXH32_reset/update/digest`), from
  `xxh32_stream.h`. Easy to get the qualification wrong.
- **Always extend the test.** A behavioral change adds an assertion to `test/flume_test.cc`.
  The round-trip equality, the partial-feed paths, and the corruption checks are the contract.

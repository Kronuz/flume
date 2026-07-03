# flume

Carry a file (or any fd-backed byte source) downstream over a byte channel: compressed,
framed, integrity-checked, in bounded memory. A flume is the wooden channel that floats logs
downstream. You feed a file in one end and it comes out the other, smaller and intact,
without ever holding the whole thing in memory.

flume is the transport-layer counterpart to the [Kronuz/compressors](https://github.com/Kronuz/compressors)
**buffer-core** codecs. compressors squeezes an in-memory buffer; flume owns the fd reading,
the on-wire framing, and the end-to-end integrity check, and delegates the per-block squeeze
to a codec policy. It is the modern replacement for the LZ4 connection codec Xapiand used for
its `FILE_FOLLOWS` database-file transfer.

Header-only, C++20, standalone.

## Why Zstd by default

The classic codec was LZ4. Zstd ships a much better ratio at competitive speed, and for a
whole-database transfer the ratio *is* the bandwidth. Measured on a 32 MB replication-like
corpus in flume's 256 KB blocks (`benchmarks/bench.cc`):

| codec          | ratio  | compress   | decompress |
| -------------- | ------ | ---------- | ---------- |
| lz4 (classic)  | 7.90x  | 1581 MB/s  | 3162 MB/s  |
| deflate/zlib   | 13.13x | 223 MB/s   | 2792 MB/s  |
| **zstd (lvl 3)** | **12.63x** | **1460 MB/s** | **3209 MB/s** |

Zstd moves **~37% fewer bytes** than LZ4 (2.66 MB vs 4.25 MB for 32 MB) at essentially the
same speed (8% slower to compress, slightly *faster* to decompress). Deflate's marginally
better ratio costs 6.5x the compression time, which a data path cannot spend. The codec is a
compile-time policy, so LZ4/Deflate are one-liners away for interop.

## The frame

One transfer is a run of length-prefixed compressed blocks, a zero-length terminator, and an
XXH32 footer over the *uncompressed* bytes:

```
[len_1][block_1] [len_2][block_2] ... [0] [xxh32]
```

Each `block_i` is `codec::compress(read_chunk_i)`, `len` is `serialise_length(block.size())`
(a varint), a zero-length block terminates the stream, and the reader verifies its running
XXH32 against the footer. Corruption (a flipped byte, a truncation) is caught, never silently
accepted.

## Use

Both ends are channel-agnostic templates. The **Sender** drives an fd into a `Writer` sink;
the **Receiver** is fed channel bytes as they arrive and writes to a `Sink`.

```cpp
#include "flume.h"

// send: stream a file down `socket_fd`, compressed + framed.
struct SocketWriter { int fd; bool write(std::string_view v) { /* write all of v */ } };
SocketWriter w{socket_fd};
flume::Sender<SocketWriter> sender(w, file_fd);
sender.send();                       // true on a complete transfer

// receive: feed socket reads in as they arrive; reconstruct into `out_fd`.
struct FdSink { int fd; bool write(const char* d, std::size_t n) { /* write all n */ } };
FdSink sink{out_fd};
flume::Receiver<FdSink> receiver(sink);
char buf[64 * 1024];
for (ssize_t n; (n = ::read(socket_fd, buf, sizeof buf)) > 0; ) {
    auto st = receiver.feed(buf, n);
    if (st == flume::Receiver<FdSink>::Status::Done) break;   // checksum verified
    if (st == flume::Receiver<FdSink>::Status::Error) { /* corrupt / truncated */ break; }
}
```

The `Writer` is where the transport plugs in: append to a reply buffer, `async_write` an Asio
socket, enqueue a libev buffer. A block that straddles two `feed()` calls, or a varint split
across them, is handled.

`examples/pipe_file.cc` streams a real 8 MB file through a flume over a `socketpair` and
checks it comes out byte-identical.

## Build

```sh
cmake -B build -DCMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH="$(brew --prefix)"
cmake --build build
ctest --test-dir build
```

Depends on [Kronuz/compressors](https://github.com/Kronuz/compressors) (fetched
automatically; `-DFETCHCONTENT_SOURCE_DIR_COMPRESSORS=<path>` for a local checkout). On macOS
the keg-only Homebrew compression libraries need `-DCMAKE_PREFIX_PATH="$(brew --prefix)"`.

## License

MIT.

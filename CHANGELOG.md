# Changelog

Notable changes to `flume`, newest first. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the library has no releases, so
entries are dated.

## 2026-11-01

### Added

- `Receiver(sink, max_block)`: an opt-in cap for Receivers that read from an untrusted
  sender. With `max_block` above 0, a declared block length above it is `Status::Error` as
  soon as the length prefix is read, a block that decodes to more than it is `Error` (a codec
  with `decompress(view, max_size)` refuses the frame from its recorded content size before
  allocating; `ZstdCodec` has that overload, built on `decompress_zstd(view, max_size)`), and
  a length prefix that is still unparsable at 11 bytes, its longest possible length, is
  `Error`. The buffer is released on `Error`, and holds at most `max_block`, one prefix and
  the bytes of one `feed()` otherwise. `max_block()` reads the cap back, and
  `max_block_for(block_size)` gives the cap that fits a Sender's block size (the block plus
  its worst-case expansion: `block_size + block_size / 128 + 1024`).
- The default is unchanged: `max_block` is 0, which behaves as before, trusting the sender's
  sizes. The README's "Not for" names that hazard and the cap.

### Tests

- `flume` (`test/flume_test.cc`) checks, without a cap, that a declared 1 TiB block is awaited
  and 4 MiB of bytes behind it is buffered, that an unparsable prefix is awaited with
  everything behind it buffered, and that a 64 MiB block of zeros (a frame under 64 KiB)
  decompresses to the Sink. With a cap it checks that the same inputs are `Error` at once with
  an empty buffer and nothing written, the boundary (`max_block` awaited, `max_block + 1`
  refused), the prefix at 10 and 11 bytes, a block that decodes past the cap, round trips of
  noise, zeros, text and incompressible data at three feed sizes and two block sizes with
  `max_block_for`, and that `leftover()` still returns the bytes past the footer.

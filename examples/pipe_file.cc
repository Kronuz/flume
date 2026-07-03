// pipe_file -- a runnable demo of flume over a real socket. It generates a file, streams it
// through a flume down one end of a socketpair (the Sender, on its own thread, in bounded
// memory) and reconstructs it at the other end (the Receiver, feeding socket reads), then
// checks the two are byte-identical and prints the on-wire size. This is the actual shape of
// the Xapiand replication use: a whole database file crossing a socket, compressed + framed.
//
//   c++ -std=c++20 -I.. -I<compressors> -I<zstd/include> pipe_file.cc <compressors>/compressor_zstd.cc -lzstd

#include <cstdio>
#include <cstdint>
#include <fcntl.h>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

#include "flume.h"

// Sink over a file fd: write the reconstructed bytes to the destination file.
struct FdSink {
	int fd;
	bool write(const char* d, std::size_t n) {
		std::size_t off = 0;
		while (off < n) {
			ssize_t w = ::write(fd, d + off, n - off);
			if (w <= 0) { return false; }
			off += static_cast<std::size_t>(w);
		}
		return true;
	}
};

// Writer over a socket fd that also counts the on-wire bytes flume emits.
struct CountingSocketWriter {
	int fd;
	std::uint64_t* wire;
	bool write(std::string_view v) {
		*wire += v.size();
		std::size_t off = 0;
		while (off < v.size()) {
			ssize_t n = ::write(fd, v.data() + off, v.size() - off);
			if (n <= 0) { return false; }
			off += static_cast<std::size_t>(n);
		}
		return true;
	}
};

int main() {
	// 1) generate a source file (~8 MB of compressible records).
	char src_tmpl[] = "/tmp/flume_src_XXXXXX";
	int src = ::mkstemp(src_tmpl);
	std::string rec = "{\"id\":42,\"note\":\"the quick brown fox jumps over the lazy dog\"}\n";
	for (int i = 0; i < 140000; ++i) { [[maybe_unused]] ssize_t w = ::write(src, rec.data(), rec.size()); }
	off_t src_size = ::lseek(src, 0, SEEK_CUR);
	::lseek(src, 0, SEEK_SET);

	char dst_tmpl[] = "/tmp/flume_dst_XXXXXX";
	int dst = ::mkstemp(dst_tmpl);

	int sv[2];
	::socketpair(AF_UNIX, SOCK_STREAM, 0, sv);

	// 2) sender thread: stream the file through a flume into sv[0].
	std::uint64_t wire_bytes = 0;
	std::thread sender([&] {
		CountingSocketWriter w{sv[0], &wire_bytes};
		flume::Sender<CountingSocketWriter> s(w, src);
		s.send();
		::shutdown(sv[0], SHUT_WR);
	});

	// 3) receiver: feed socket reads into the flume until Done.
	FdSink sink{dst};
	flume::Receiver<FdSink> receiver(sink);
	char buf[64 * 1024];
	using Status = flume::Receiver<FdSink>::Status;
	Status st = Status::NeedMore;
	for (;;) {
		ssize_t n = ::read(sv[1], buf, sizeof(buf));
		if (n <= 0) { break; }
		st = receiver.feed(buf, static_cast<std::size_t>(n));
		if (st == Status::Done || st == Status::Error) { break; }
	}
	sender.join();

	// 4) verify byte-identical.
	::fsync(dst);
	off_t dst_size = ::lseek(dst, 0, SEEK_END);
	bool ok = (st == Status::Done) && (dst_size == src_size);
	std::printf("source      : %lld bytes\n", static_cast<long long>(src_size));
	std::printf("on the wire : %llu bytes  (%.2fx smaller)\n", static_cast<unsigned long long>(wire_bytes),
		static_cast<double>(src_size) / static_cast<double>(wire_bytes != 0u ? wire_bytes : 1));
	std::printf("received    : %lld bytes  -> %s\n", static_cast<long long>(dst_size), ok ? "OK, identical" : "MISMATCH");

	::close(src); ::close(dst); ::close(sv[0]); ::close(sv[1]);
	::unlink(src_tmpl); ::unlink(dst_tmpl);
	return ok ? 0 : 1;
}

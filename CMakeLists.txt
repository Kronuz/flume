# flume: carry a file downstream over a byte channel as compressed, framed, integrity-checked
# blocks, in bounded memory. The transport-layer counterpart to the Kronuz/compressors
# buffer-core codecs (Zstd by default).
#
#   cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build && ctest --test-dir build
#
# Header-only. On macOS the compression libraries are keg-only Homebrew formulae, so pass
# -DCMAKE_PREFIX_PATH="$(brew --prefix)" so find_library sees them (compressors needs it too).

cmake_minimum_required(VERSION 3.16)
project(flume LANGUAGES CXX)

include(FetchContent)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE)
	set(CMAKE_BUILD_TYPE Release)
endif()

# --- compressors: the buffer-core codecs (Zstd/LZ4/Deflate) + the vendored streaming XXH32 ---
# Provides compressors::compressors (compress_zstd/decompress_zstd, xxh32_stream.h) and links
# zstd/lz4/zlib. Set -DFETCHCONTENT_SOURCE_DIR_COMPRESSORS=<path> to build against a checkout.
FetchContent_Declare(compressors GIT_REPOSITORY https://github.com/Kronuz/compressors.git GIT_TAG main)
FetchContent_MakeAvailable(compressors)

# --- the flume header-only lib ---
add_library(flume INTERFACE)
target_include_directories(flume INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}")
target_compile_features(flume INTERFACE cxx_std_20)
target_link_libraries(flume INTERFACE compressors::compressors)
add_library(flume::flume ALIAS flume)

# --- example + benchmark + test (built only when this repo is top-level) ---
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
	enable_testing()

	# Stream a file through a flume over a socketpair; verify byte-identical + show the ratio.
	add_executable(flume_pipe_file examples/pipe_file.cc)
	target_link_libraries(flume_pipe_file PRIVATE flume)

	# Is Zstd worth it over the classic LZ4? Ratio + throughput on a replication-like corpus.
	add_executable(flume_bench benchmarks/bench.cc)
	target_link_libraries(flume_bench PRIVATE flume)

	# Round-trip + partial-feed + corruption-detection.
	add_executable(flume_test test/flume_test.cc)
	target_link_libraries(flume_test PRIVATE flume)
	add_test(NAME flume COMMAND flume_test)
endif()

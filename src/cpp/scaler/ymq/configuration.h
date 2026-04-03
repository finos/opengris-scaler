#pragma once

#include <chrono>
#include <cstddef>

namespace scaler {
namespace ymq {

constexpr size_t defaultClientMaxRetryTimes = 4;
constexpr std::chrono::milliseconds defaultClientInitRetryDelay {2000};

constexpr int serverListenBacklog = 1024;

// Maximum buffer size for a single write() syscall.
//
// Some OSes discourage large writes (macOS, Windows).
constexpr size_t maxWriteBufferSize = 256ULL * 1024ULL * 1024ULL;  // 256 MB

}  // namespace ymq
}  // namespace scaler

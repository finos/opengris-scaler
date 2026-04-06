#pragma once

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>

namespace scaler {
namespace ymq {

// Expect all connections to start with this string.
constexpr std::array<uint8_t, 4> magicString {'Y', 'M', 'Q', 1};

constexpr size_t defaultClientMaxRetryTimes = 4;
constexpr std::chrono::milliseconds defaultClientInitRetryDelay {2000};

constexpr int serverListenBacklog = 1024;

// Maximum buffer size for a single write() syscall.
//
// Some OSes discourage large writes (macOS, Windows).
constexpr size_t maxWriteBufferSize = 256ULL * 1024ULL * 1024ULL;  // 256 MB

}  // namespace ymq
}  // namespace scaler

#pragma once

#include <chrono>

namespace scaler {
namespace uv_ymq {

constexpr size_t DEFAULT_MAX_RETRY_TIMES = 4;
constexpr std::chrono::milliseconds DEFAULT_INIT_RETRY_DELAY {2000};

}  // namespace uv_ymq
}  // namespace scaler

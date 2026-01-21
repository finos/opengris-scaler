#pragma once

#include <chrono>

namespace scaler {
namespace uv_ymq {

constexpr size_t DEFAULT_CLIENT_MAX_RETRY_TIMES = 4;
constexpr std::chrono::milliseconds DEFAULT_CLIENT_INIT_RETRY_DELAY {2000};

constexpr int DEFAULT_SERVER_LISTEN_BACKLOG = 1024;

}  // namespace uv_ymq
}  // namespace scaler

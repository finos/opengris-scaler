#pragma once

#include <chrono>

namespace scaler {
namespace ymq {

constexpr size_t defaultClientMaxRetryTimes = 4;
constexpr std::chrono::milliseconds defaultClientInitRetryDelay {2000};

constexpr int serverListenBacklog = 1024;

}  // namespace ymq
}  // namespace scaler

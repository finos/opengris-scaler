#pragma once

#include <uv.h>

#include <cstdint>
#include <expected>
#include <span>

#include "scaler/utility/move_only_function.h"
#include "scaler/wrapper/uv/error.h"

namespace scaler {
namespace wrapper {
namespace uv {

// Provides higher level C++ abstractions over libuv callbacks (uv_*_cb), through the use of MoveOnlyFunction and
// std::expected.

// See uv_async_cb
using AsyncCallback = scaler::utility::MoveOnlyFunction<void()>;

// See uv_read_cb
//
// The std::span buffer is valid only during the execution of this callback.
using ReadCallback = scaler::utility::MoveOnlyFunction<void(std::expected<std::span<const uint8_t>, Error> result)>;

// See uv_connect_cb
using ConnectCallback = scaler::utility::MoveOnlyFunction<void(std::expected<void, Error> result)>;

// See uv_connection_cb
using ConnectionCallback = scaler::utility::MoveOnlyFunction<void(std::expected<void, Error> result)>;

// See uv_signal_cb
using SignalCallback = scaler::utility::MoveOnlyFunction<void(int signum)>;

// See uv_shutdown_cb
using ShutdownCallback = scaler::utility::MoveOnlyFunction<void(std::expected<void, Error> result)>;

// See uv_timer_cb
using TimerCallback = scaler::utility::MoveOnlyFunction<void()>;

// See uv_write_cb
using WriteCallback = scaler::utility::MoveOnlyFunction<void(std::expected<void, Error> result)>;

}  // namespace uv
}  // namespace wrapper
}  // namespace scaler

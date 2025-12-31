#pragma once

#include <uv.h>

#include <cstdint>
#include <expected>
#include <span>

#include "scaler/utility/move_only_function.h"
#include "scaler/uv/error.h"

namespace scaler {
namespace uv {

// Provides higher level C++ abstractions over libuv callbacks (uv_*_cb), through the use of MoveOnlyFunction and
// std::expected.

// See uv_async_cb
using AsyncCallback = utility::MoveOnlyFunction<void()>;

// See uv_read_cb
//
// The std::span buffer is valid only during the execution of this callback.
using ReadCallback = utility::MoveOnlyFunction<void(std::expected<std::span<uint8_t>, Error>&& result)>;

// See uv_connect_cb
using ConnectCallback = utility::MoveOnlyFunction<void(std::expected<void, Error>&& result)>;

// See uv_connection_cb
using ConnectionCallback = utility::MoveOnlyFunction<void(std::expected<void, Error>&& result)>;

// See uv_signal_cb
using SignalCallback = utility::MoveOnlyFunction<void(int signum)>;

// See uv_shutdown_cb
using ShutdownCallback = utility::MoveOnlyFunction<void(std::expected<void, Error>&& result)>;

// See uv_timer_cb
using TimerCallback = utility::MoveOnlyFunction<void()>;

// See uv_write_cb
using WriteCallback = utility::MoveOnlyFunction<void(std::expected<void, Error>&& result)>;

}  // namespace uv
}  // namespace scaler

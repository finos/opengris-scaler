#pragma once

#include <uv.h>

#include <chrono>
#include <expected>
#include <optional>

#include "scaler/wrapper/uv/callback.h"
#include "scaler/wrapper/uv/error.h"
#include "scaler/wrapper/uv/handle.h"
#include "scaler/wrapper/uv/loop.h"

namespace scaler {
namespace wrapper {
namespace uv {

// See uv_timer_t
class Timer {
public:
    // See uv_timer_init
    static std::expected<Timer, Error> init(Loop& loop) noexcept;

    // See uv_timer_start
    std::expected<void, Error> start(
        std::chrono::milliseconds timeout,
        std::optional<std::chrono::milliseconds> repeat,
        TimerCallback callback) noexcept;

    // See uv_timer_stop
    std::expected<void, Error> stop() noexcept;

    // See uv_timer_again
    std::expected<void, Error> again() noexcept;

    // See uv_timer_set_repeat
    void setRepeat(std::chrono::milliseconds repeat) noexcept;

    // See uv_timer_get_repeat
    std::optional<std::chrono::milliseconds> getRepeat() const noexcept;

private:
    Handle<uv_timer_t, TimerCallback> _handle;

    Timer() noexcept = default;

    static void onTimerCallback(uv_timer_t* timer) noexcept;
};

}  // namespace uv
}  // namespace wrapper
}  // namespace scaler

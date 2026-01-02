#pragma once

#include <uv.h>

#include "scaler/utility/move_only_function.h"
#include "scaler/uv/error.h"
#include "scaler/uv/handle.h"
#include "scaler/uv/loop.h"

namespace scaler {
namespace uv {

// See uv_signal_t
class Signal {
public:
    using Callback = utility::MoveOnlyFunction<void(int)>;

    // See uv_signal_init
    static std::expected<Signal, Error> init(Loop& loop) noexcept;

    // See uv_signal_start
    std::expected<void, Error> start(int signum, Callback&& callback) noexcept;

    // See uv_signal_start_oneshot
    std::expected<void, Error> startOneshot(int signum, Callback&& callback) noexcept;

    // See uv_signal_stop
    std::expected<void, Error> stop() noexcept;

private:
    Handle<uv_signal_t, Callback> _handle;

    Signal() noexcept = default;

    static void onSignalCallback(uv_signal_t* signal, int signum) noexcept;
};

}  // namespace uv
}  // namespace scaler

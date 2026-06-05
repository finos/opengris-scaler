#pragma once

#include <uv.h>

#include "scaler/wrapper/uv/callback.h"
#include "scaler/wrapper/uv/error.h"
#include "scaler/wrapper/uv/handle.h"
#include "scaler/wrapper/uv/loop.h"

namespace scaler {
namespace wrapper {
namespace uv {

// See uv_signal_t
class Signal {
public:
    // See uv_signal_init
    static std::expected<Signal, Error> init(Loop& loop) noexcept;

    // See uv_signal_start
    std::expected<void, Error> start(int signum, SignalCallback callback) noexcept;

    // See uv_signal_start_oneshot
    std::expected<void, Error> startOneshot(int signum, SignalCallback callback) noexcept;

    // See uv_signal_stop
    std::expected<void, Error> stop() noexcept;

private:
    Handle<uv_signal_t, SignalCallback> _handle;

    Signal() noexcept = default;

    static void onSignalCallback(uv_signal_t* signal, int signum) noexcept;
};

}  // namespace uv
}  // namespace wrapper
}  // namespace scaler

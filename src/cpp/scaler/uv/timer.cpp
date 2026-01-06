#include "scaler/uv/timer.h"

#include <cassert>
#include <cstdint>

namespace scaler {
namespace uv {

std::expected<Timer, Error> Timer::init(Loop& loop) noexcept
{
    Timer timer;

    int err = uv_timer_init(&loop.native(), &timer._handle.native());
    if (err) {
        return std::unexpected(Error {err});
    }

    return timer;
}

std::expected<void, Error> Timer::start(
    std::chrono::milliseconds timeout, std::optional<std::chrono::milliseconds> repeat, Callback&& callback) noexcept
{
    _handle.setData(std::move(callback));

    uint64_t repeatNative = repeat.has_value() ? repeat->count() : 0;

    int err = uv_timer_start(&_handle.native(), &onTimerCallback, timeout.count(), repeatNative);
    if (err) {
        return std::unexpected(Error {err});
    }

    return {};
}

std::expected<void, Error> Timer::stop() noexcept
{
    int err = uv_timer_stop(&_handle.native());
    if (err) {
        return std::unexpected(Error {err});
    }

    return {};
}

std::expected<void, Error> Timer::again() noexcept
{
    int err = uv_timer_again(&_handle.native());
    if (err) {
        return std::unexpected(Error {err});
    }

    return {};
}

void Timer::setRepeat(std::chrono::milliseconds repeat) noexcept
{
    uv_timer_set_repeat(&_handle.native(), repeat.count());
}

std::optional<std::chrono::milliseconds> Timer::getRepeat() const noexcept
{
    uint64_t nativeRepeat = uv_timer_get_repeat(&_handle.native());

    if (nativeRepeat == 0) {
        return std::nullopt;
    } else {
        return {std::chrono::milliseconds(nativeRepeat)};
    }
}

void Timer::onTimerCallback(uv_timer_t* timer) noexcept
{
    Callback* callback = reinterpret_cast<Callback*>(uv_handle_get_data(reinterpret_cast<uv_handle_t*>(timer)));

    assert(callback != nullptr);

    (*callback)();
}

}  // namespace uv
}  // namespace scaler
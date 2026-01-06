#include "scaler/uv/signal.h"

#include <cassert>

namespace scaler {
namespace uv {

std::expected<Signal, Error> Signal::init(Loop& loop) noexcept
{
    Signal signal;

    int err = uv_signal_init(&loop.native(), &signal._handle.native());
    if (err) {
        return std::unexpected(Error {err});
    }

    return signal;
}

std::expected<void, Error> Signal::start(int signum, Callback&& callback) noexcept
{
    _handle.setData(std::move(callback));

    int err = uv_signal_start(&_handle.native(), &onSignalCallback, signum);
    if (err) {
        return std::unexpected(Error {err});
    }

    return {};
}

std::expected<void, Error> Signal::startOneshot(int signum, Callback&& callback) noexcept
{
    _handle.setData(std::move(callback));

    int err = uv_signal_start_oneshot(&_handle.native(), &onSignalCallback, signum);
    if (err) {
        return std::unexpected(Error {err});
    }

    return {};
}

std::expected<void, Error> Signal::stop() noexcept
{
    int err = uv_signal_stop(&_handle.native());
    if (err) {
        return std::unexpected(Error {err});
    }

    return {};
}

void Signal::onSignalCallback(uv_signal_t* signal, int signum) noexcept
{
    Callback* callback = reinterpret_cast<Callback*>(uv_handle_get_data(reinterpret_cast<uv_handle_t*>(signal)));

    assert(callback != nullptr);

    (*callback)(signum);
}

}  // namespace uv
}  // namespace scaler

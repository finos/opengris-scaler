#include "scaler/wrapper/uv/async.h"

#include <cassert>

namespace scaler {
namespace wrapper {
namespace uv {

std::expected<Async, Error> Async::init(Loop& loop, std::optional<AsyncCallback> callback) noexcept
{
    uv_async_cb nativeCallback {};

    if (callback.has_value()) {
        nativeCallback = &onAsyncCallback;
    } else {
        nativeCallback = nullptr;
    }

    Async async;

    const int err = uv_async_init(&loop.native(), &async._handle.native(), nativeCallback);
    if (err) {
        return std::unexpected {Error {err}};
    }

    if (callback.has_value()) {
        async._handle.setData(std::move(*callback));
    }

    return async;
}

std::expected<void, Error> Async::send() noexcept
{
    const int err = uv_async_send(&_handle.native());
    if (err) {
        return std::unexpected {Error {err}};
    }

    return {};
}

void Async::onAsyncCallback(uv_async_t* async) noexcept
{
    AsyncCallback* callback =
        reinterpret_cast<AsyncCallback*>(uv_handle_get_data(reinterpret_cast<uv_handle_t*>(async)));

    assert(callback != nullptr);

    (*callback)();
}

}  // namespace uv
}  // namespace wrapper
}  // namespace scaler

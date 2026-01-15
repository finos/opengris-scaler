#pragma once

#include <uv.h>

#include <expected>

#include "scaler/wrapper/uv/callback.h"
#include "scaler/wrapper/uv/error.h"
#include "scaler/wrapper/uv/handle.h"
#include "scaler/wrapper/uv/loop.h"

namespace scaler {
namespace wrapper {
namespace uv {

// See uv_async_t
class Async {
public:
    // See uv_async_init
    static std::expected<Async, Error> init(Loop& loop, std::optional<AsyncCallback> callback) noexcept;

    // See uv_async_send
    std::expected<void, Error> send() noexcept;

private:
    Handle<uv_async_t, AsyncCallback> _handle;

    Async() noexcept = default;

    static void onAsyncCallback(uv_async_t* async) noexcept;
};

}  // namespace uv
}  // namespace wrapper
}  // namespace scaler

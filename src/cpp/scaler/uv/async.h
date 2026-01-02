#pragma once

#include <uv.h>

#include <expected>

#include "scaler/utility/move_only_function.h"
#include "scaler/uv/error.h"
#include "scaler/uv/handle.h"
#include "scaler/uv/loop.h"

namespace scaler {
namespace uv {

// See uv_async_t
class Async {
public:
    using Callback = utility::MoveOnlyFunction<void()>;

    // See uv_async_init
    static std::expected<Async, Error> init(Loop& loop, std::optional<Callback>&& asyncCallback) noexcept;

    // See uv_async_send
    std::expected<void, Error> send() noexcept;

private:
    Handle<uv_async_t, Callback> _handle;

    Async() noexcept = default;

    static void onAsyncCallback(uv_async_t* async) noexcept;
};

}  // namespace uv
}  // namespace scaler

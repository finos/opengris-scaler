#pragma once

#include <uv.h>

#include <expected>

#include "scaler/event/error.h"
#include "scaler/event/handle.h"
#include "scaler/event/loop.h"
#include "scaler/utility/move_only_function.h"

namespace scaler {
namespace event {

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

}  // namespace event
}  // namespace scaler

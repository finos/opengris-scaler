#pragma once

#include <uv.h>

#include <cassert>
#include <expected>
#include <memory>

#include "scaler/utility/move_only_function.h"
#include "scaler/uv/error.h"

namespace scaler {
namespace uv {

// A RAII holder for all libuv request classes (uv_connect_t, uv_shutdown_t, uv_, uv_write_t ...).
//
// The wrapped libuv request object will be deallocated once the request's callback has been called AND all Request
// objects associated with it have been destructed.
template <typename NativeRequestType, typename... CallbackArgs>
class Request {
public:
    using CallbackType = void(CallbackArgs...);

    Request(utility::MoveOnlyFunction<CallbackType>&& callback) noexcept
    {
        _holder->callback = std::move(callback);

        // Add a self-owning reference to the holder to ensure it is not destroyed before the callback is called.
        _holder->self = _holder;
        assert(_holder.use_count() == 2);

        uv_req_set_data(reinterpret_cast<uv_req_t*>(&_holder->native), _holder.get());
    }

    constexpr NativeRequestType& native() noexcept { return _holder->native; }

    constexpr const NativeRequestType& native() const noexcept { return _holder->native; }

    // See uv_req_cancel
    std::expected<void, Error> cancel() noexcept
    {
        int err = uv_cancel(reinterpret_cast<uv_req_t*>(&_holder->native));
        if (err) {
            return std::unexpected(Error {err});
        }
        return {};
    }

    // The libuv callback to register when calling uv_write(), uv_read(), uv_shutdown()...
    //
    // This will call then release the callback provided to the object's constructor.
    static void onCallback(NativeRequestType* request, CallbackArgs... args) noexcept
    {
        Holder* holder = static_cast<Holder*>(request->data);
        assert(holder->self != nullptr);  // libuv should only call the callback once

        holder->callback(args...);

        // Release the callback object.
        holder->callback = {};

        // Release the self-owning reference. This reduces the ref-count of the holder, and possibly deallocates it.
        holder->self = nullptr;
    }

private:
    struct Holder {
        NativeRequestType native;
        utility::MoveOnlyFunction<CallbackType> callback;

        // Maintain a self-owning reference until the callback is called. Required for RAII.
        std::shared_ptr<Holder> self;
    };

    std::shared_ptr<Holder> _holder {std::make_shared<Holder>()};
};

// See uv_connect_t
using ConnectRequest = Request<uv_connect_t, int>;

// See uv_shutdown_t
using ShutdownRequest = Request<uv_shutdown_t, int>;

// See uv_write_t
using WriteRequest = Request<uv_write_t, int>;

}  // namespace uv
}  // namespace scaler

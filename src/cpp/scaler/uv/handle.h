#pragma once

#include <uv.h>

#include <memory>

namespace scaler {
namespace uv {

// A RAII holder for all libuv handle classes (uv_timer_t, uv_tcp_t ...).
//
// Destructing this object will ultimately close the underlying libuv handle object and cancel any pending operations
// (see uv_close()).
template <typename NativeHandleType, typename DataType>
class Handle {
public:
    constexpr NativeHandleType& native() noexcept { return *_native; }

    constexpr const NativeHandleType& native() const noexcept { return *_native; }

    // See uv_handle_get_data
    DataType& data() noexcept
    {
        const uv_handle_t* nativeHandle = reinterpret_cast<uv_handle_t*>(&native());

        return *static_cast<DataType*>(uv_handle_get_data(nativeHandle));
    }

    // See uv_handle_set_data
    void setData(DataType&& value) noexcept
    {
        uv_handle_t* nativeHandle = reinterpret_cast<uv_handle_t*>(&native());

        if (uv_handle_get_data(nativeHandle) == nullptr) {
            uv_handle_set_data(nativeHandle, new DataType(std::move(value)));
        } else {
            DataType* data = static_cast<DataType*>(uv_handle_get_data(nativeHandle));

            *data = std::move(value);  // overriding
        }
    }

private:
    // We cannot hold the native handle object directly because uv_close() "delays" the deletion of the native handle.
    // It has to be freed while/after uv_close()'s callback is being called.
    //
    // By heap-allocating the native libuv handle, we allow it to "outlive" the Handle instance.

    static void handleDeleter(NativeHandleType* handle) noexcept
    {
        // Delay the call to delete to uv_close()'s callback.
        uv_close(reinterpret_cast<uv_handle_t*>(handle), [](uv_handle_t* handle) {
            DataType* data = static_cast<DataType*>(uv_handle_get_data(handle));

            if (data != nullptr) {
                delete data;
            }

            delete handle;
        });
    }

    std::unique_ptr<NativeHandleType, decltype(&handleDeleter)> _native {
        new NativeHandleType(), &Handle::handleDeleter};
};

}  // namespace uv
}  // namespace scaler

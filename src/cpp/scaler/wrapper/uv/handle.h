#pragma once

#include <uv.h>

#include <cassert>
#include <memory>

namespace scaler {
namespace wrapper {
namespace uv {

// A RAII holder for all libuv handle classes (uv_timer_t, uv_tcp_t ...).
//
// Destructing this object will ultimately close the underlying libuv handle object and cancel any pending operations
// (see uv_close()).
template <typename NativeHandleType, typename DataType>
class Handle {
public:
    constexpr NativeHandleType& native() noexcept
    {
        assert(_native != nullptr && "The handle has been moved or released");
        return *_native;
    }

    constexpr const NativeHandleType& native() const noexcept
    {
        assert(_native != nullptr && "The handle has been moved or released");
        return *_native;
    }

    // See uv_handle_get_data
    DataType& data() noexcept
    {
        const uv_handle_t* nativeHandle = reinterpret_cast<uv_handle_t*>(&native());

        return *static_cast<DataType*>(uv_handle_get_data(nativeHandle));
    }

    // See uv_handle_set_data
    void setData(DataType value) noexcept
    {
        uv_handle_t* nativeHandle = reinterpret_cast<uv_handle_t*>(&native());

        if (uv_handle_get_data(nativeHandle) == nullptr) {
            uv_handle_set_data(nativeHandle, new DataType(std::move(value)));
        } else {
            DataType* data = static_cast<DataType*>(uv_handle_get_data(nativeHandle));

            *data = std::move(value);  // overriding
        }
    }

    // Releases the ownership of the native handle.
    //
    // The caller is responsible for freeing the native handle by calling free().
    NativeHandleType* release() noexcept { return _native.release(); }

    // Callback for uv_close() that cleans up the handle data and deletes the native handle.
    static void free(uv_handle_t* handle) noexcept
    {
        DataType* data = static_cast<DataType*>(uv_handle_get_data(handle));
        delete data;

        delete reinterpret_cast<NativeHandleType*>(handle);
    }

private:
    // We cannot hold the native handle object directly because uv_close() "delays" the deletion of the native handle.
    // It has to be freed while/after uv_close()'s callback is being called.
    //
    // By heap-allocating the native libuv handle, we allow it to "outlive" the Handle instance.

    static void handleDeleter(NativeHandleType* handle) noexcept
    {
        uv_handle_t* rawHandle = reinterpret_cast<uv_handle_t*>(handle);

        // Skip if the handle is already closing (e.g. from TCPSocket::closeReset())
        if (uv_is_closing(rawHandle)) {
            return;
        }

        // Delay the call to delete to uv_close()'s callback.
        uv_close(rawHandle, &Handle::free);
    }

    std::unique_ptr<NativeHandleType, decltype(&handleDeleter)> _native {
        new NativeHandleType(), &Handle::handleDeleter};
};

}  // namespace uv
}  // namespace wrapper
}  // namespace scaler

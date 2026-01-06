#pragma once

#include <uv.h>

#include <cstdint>
#include <expected>
#include <memory>
#include <span>
#include <vector>

#include "scaler/uv/callback.h"
#include "scaler/uv/handle.h"
#include "scaler/uv/request.h"

namespace scaler {
namespace uv {

// A base class for uv_stream_t wrappers and their descandents (uv_tcp_t, uv_pipe_t ...).
template <typename NativeHandleType, typename DataType>
class Stream {
public:
    // See uv_shutdown
    std::expected<ShutdownRequest, Error> shutdown(ShutdownCallback&& callback) noexcept
    {
        ShutdownRequest request([callback = std::move(callback)](int status) mutable {
            if (status < 0) {
                callback(std::unexpected {Error {status}});
            } else {
                callback({});
            }
        });

        int err = uv_shutdown(
            &request.native(), reinterpret_cast<uv_stream_t*>(&handle().native()), ShutdownRequest::onCallback);

        if (err) {
            return std::unexpected(Error {err});
        }

        return request;
    }

    constexpr Handle<NativeHandleType, DataType>& handle() noexcept { return _handle; }

    constexpr const Handle<NativeHandleType, DataType>& handle() const noexcept { return _handle; }

private:
    Handle<NativeHandleType, DataType> _handle;
};

template <typename NativeHandleType>
class ConnectingStream: public Stream<NativeHandleType, ReadCallback> {
public:
    // See uv_read_start
    std::expected<void, Error> readStart(ReadCallback&& callback) noexcept
    {
        this->handle().setData(std::move(callback));

        int err = uv_read_start(
            reinterpret_cast<uv_stream_t*>(&this->handle().native()), &onAllocateCallback, &onReadCallback);
        if (err) {
            return std::unexpected(Error {err});
        }

        return {};
    }

    // See uv_read_stop
    void readStop() noexcept
    {
        uv_read_stop(reinterpret_cast<uv_stream_t*>(&this->handle().native()));

        this->handle().setData(ReadCallback());  // force destruction of the callback object
    }

    // See uv_write
    //
    // The buffers' content (inner std::span<uint8_t>) must remain valid until the callback is called. The user is
    // responsible for freeing these buffers.
    std::expected<WriteRequest, Error> write(
        std::span<std::span<const uint8_t>> buffers, WriteCallback&& callback) noexcept
    {
        std::vector<uv_buf_t> nativeBuffers {};
        nativeBuffers.reserve(buffers.size());

        for (auto const& buffer: buffers) {
            uv_buf_t nativeBuffer = uv_buf_init(
                const_cast<char*>(reinterpret_cast<const char*>(buffer.data())),
                static_cast<unsigned int>(buffer.size()));

            nativeBuffers.push_back(nativeBuffer);
        }

        WriteRequest request([callback = std::move(callback)](int status) mutable {
            if (status == 0) {
                callback({});
            } else {
                callback(std::unexpected {Error {status}});
            }
        });

        int err = uv_write(
            &request.native(),
            reinterpret_cast<uv_stream_t*>(&this->handle().native()),
            nativeBuffers.data(),
            static_cast<unsigned int>(nativeBuffers.size()),
            &WriteRequest::onCallback);

        if (err) {
            return std::unexpected(Error {err});
        }

        return request;
    }

    // A single buffer alternative to write().
    std::expected<WriteRequest, Error> write(std::span<const uint8_t> buffer, WriteCallback&& callback) noexcept
    {
        return write(std::span<std::span<const uint8_t>>(&buffer, 1), std::move(callback));
    }

private:
    static void onAllocateCallback(uv_handle_t* handle, size_t suggestedSize, uv_buf_t* nativeBuffer) noexcept
    {
        *nativeBuffer = uv_buf_init(new char[suggestedSize], suggestedSize);
    }

    static void onReadCallback(uv_stream_t* stream, ssize_t nread, const uv_buf_t* buffer) noexcept
    {
        ReadCallback* callback = static_cast<ReadCallback*>(stream->data);

        if (nread < 0) {
            (*callback)(std::unexpected {Error {static_cast<int>(nread)}});
        } else {
            (*callback)(std::span<uint8_t> {reinterpret_cast<uint8_t*>(buffer->base), static_cast<size_t>(nread)});
        }

        if (buffer->base != nullptr) {
            delete[] buffer->base;
        }
    }
};

template <typename HandleType, typename ConnectionType>
class ServerStream: public Stream<HandleType, ConnectionCallback> {
public:
    // See uv_listen
    std::expected<void, Error> listen(int backlog, ConnectionCallback&& callback) noexcept
    {
        this->handle().setData(std::move(callback));

        int err = uv_listen(reinterpret_cast<uv_stream_t*>(&this->handle().native()), backlog, &onConnectionCallback);
        if (err) {
            return std::unexpected(Error {err});
        }

        return {};
    }

    // See uv_accept
    std::expected<void, Error> accept(ConnectionType& connection) noexcept
    {
        int err = uv_accept(
            reinterpret_cast<uv_stream_t*>(&this->handle().native()),
            reinterpret_cast<uv_stream_t*>(&connection.handle().native()));
        if (err < 0) {
            return std::unexpected(Error {err});
        }

        return {};
    }

private:
    static void onConnectionCallback(uv_stream_t* stream, int status)
    {
        ConnectionCallback* callback = static_cast<ConnectionCallback*>(stream->data);

        if (status < 0) {
            (*callback)(std::unexpected {Error {status}});
        } else {
            (*callback)({});
        }
    }
};

}  // namespace uv
}  // namespace scaler

#include "scaler/wrapper/uv/pipe.h"

#include <cassert>
#include <cstring>

namespace scaler {
namespace wrapper {
namespace uv {

namespace details {

std::expected<std::string, Error> getSockName(const uv_pipe_t& handle) noexcept
{
    // Get the required buffer size by calling uv_pipe_getsockname with a dummy 1-byte buffer.
    // This should return a UV_ENOBUFS error and the required buffer length.
    char dummy[1];  // Dummy buffer
    size_t len = sizeof(dummy);
    int err    = uv_pipe_getsockname(&handle, dummy, &len);
    if (err && err != UV_ENOBUFS) {
        return std::unexpected(Error {err});
    }

    // Allocate buffer with the required size
    std::string buffer;
    buffer.resize(len);

    err = uv_pipe_getsockname(&handle, buffer.data(), &len);
    if (err) {
        return std::unexpected(Error {err});
    }

    buffer.resize(len);
    return buffer;
}

std::expected<std::string, Error> getPeerName(const uv_pipe_t& handle) noexcept
{
    // Get the required buffer size by calling uv_pipe_getpeername with a dummy 1-byte buffer.
    // This should return a UV_ENOBUFS error and the required buffer length.
    char dummy[1];  // Dummy buffer
    size_t len = sizeof(dummy);
    int err    = uv_pipe_getpeername(&handle, dummy, &len);
    if (err && err != UV_ENOBUFS) {
        return std::unexpected(Error {err});
    }

    // Allocate buffer with the required size
    std::string buffer;
    buffer.resize(len);

    err = uv_pipe_getpeername(&handle, buffer.data(), &len);
    if (err) {
        return std::unexpected(Error {err});
    }

    buffer.resize(len);
    return buffer;
}

}  // namespace details

std::expected<Pipe, Error> Pipe::init(Loop& loop, bool ipc) noexcept
{
    Pipe pipe;

    int err = uv_pipe_init(&loop.native(), &pipe.handle().native(), ipc ? 1 : 0);
    if (err) {
        return std::unexpected {Error {err}};
    }

    return pipe;
}

std::expected<ConnectRequest, Error> Pipe::connect(
    const std::string& name, ConnectCallback&& callback, unsigned int flags) noexcept
{
    ConnectRequest request([callback = std::move(callback)](int status) mutable {
        if (status < 0) {
            callback(std::unexpected {Error {status}});
        } else {
            callback({});
        }
    });

    int err = uv_pipe_connect2(
        &request.native(), &handle().native(), name.c_str(), name.size(), flags, &ConnectRequest::onCallback);
    if (err) {
        request.release();
        return std::unexpected {Error {err}};
    }

    return request;
}

std::expected<std::string, Error> Pipe::getSockName() const noexcept
{
    return details::getSockName(handle().native());
}

std::expected<std::string, Error> Pipe::getPeerName() const noexcept
{
    return details::getPeerName(handle().native());
}

std::expected<PipeServer, Error> PipeServer::init(Loop& loop, bool ipc) noexcept
{
    PipeServer server;

    int err = uv_pipe_init(&loop.native(), &server.handle().native(), ipc ? 1 : 0);
    if (err) {
        return std::unexpected {Error {err}};
    }

    return server;
}

std::expected<void, Error> PipeServer::bind(const std::string& name) noexcept
{
    int err = uv_pipe_bind(&handle().native(), name.c_str());
    if (err) {
        return std::unexpected(Error {err});
    }

    return {};
}

std::expected<std::string, Error> PipeServer::getSockName() const noexcept
{
    return details::getSockName(handle().native());
}

}  // namespace uv
}  // namespace wrapper
}  // namespace scaler

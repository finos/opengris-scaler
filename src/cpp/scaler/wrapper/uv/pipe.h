#pragma once

#include <uv.h>

#include <expected>
#include <string>

#include "scaler/wrapper/uv/callback.h"
#include "scaler/wrapper/uv/error.h"
#include "scaler/wrapper/uv/loop.h"
#include "scaler/wrapper/uv/request.h"
#include "scaler/wrapper/uv/stream.h"

namespace scaler {
namespace wrapper {
namespace uv {

// See uv_pipe_t
class Pipe: public Stream<uv_pipe_t> {
public:
    // See uv_pipe_init
    static std::expected<Pipe, Error> init(Loop& loop, bool ipc) noexcept;

    // See uv_pipe_connect2
    std::expected<ConnectRequest, Error> connect(
        const std::string& name, ConnectCallback&& callback, unsigned int flags = 0) noexcept;

    // See uv_pipe_getsockname
    std::expected<std::string, Error> getSockName() const noexcept;

    // See uv_pipe_getpeername
    std::expected<std::string, Error> getPeerName() const noexcept;

private:
    Pipe() noexcept = default;
};

// See uv_pipe_t
class PipeServer: public StreamServer<uv_pipe_t, Pipe> {
public:
    // See uv_pipe_init
    static std::expected<PipeServer, Error> init(Loop& loop, bool ipc) noexcept;

    // See uv_pipe_bind
    std::expected<void, Error> bind(const std::string& name) noexcept;

    // See uv_pipe_getsockname
    std::expected<std::string, Error> getSockName() const noexcept;

private:
    PipeServer() noexcept = default;
};

}  // namespace uv
}  // namespace wrapper
}  // namespace scaler

#pragma once

#include <uv.h>

#include <expected>

#include "scaler/wrapper/uv/callback.h"
#include "scaler/wrapper/uv/error.h"
#include "scaler/wrapper/uv/loop.h"
#include "scaler/wrapper/uv/request.h"
#include "scaler/wrapper/uv/socket_address.h"
#include "scaler/wrapper/uv/stream.h"

namespace scaler {
namespace wrapper {
namespace uv {

// See uv_tcp_t
class TCPSocket: public Stream<uv_tcp_t> {
public:
    // See uv_tcp_init
    static std::expected<TCPSocket, Error> init(Loop& loop) noexcept;

    // uv_tcp_connect
    std::expected<ConnectRequest, Error> connect(const SocketAddress& address, ConnectCallback callback) noexcept;

    // See uv_tcp_close_reset
    //
    // This immediately close the socket.
    std::expected<void, Error> closeReset() noexcept;

    // See uv_tcp_getsockname
    std::expected<SocketAddress, Error> getSockName() const noexcept;

    // See uv_tcp_getpeername
    std::expected<SocketAddress, Error> getPeerName() const noexcept;

    // See uv_tcp_nodelay
    std::expected<void, Error> nodelay(bool enable) noexcept;

private:
    TCPSocket() noexcept = default;
};

// See uv_tcp_t
class TCPServer: public StreamServer<uv_tcp_t, TCPSocket> {
public:
    // See uv_tcp_init
    static std::expected<TCPServer, Error> init(Loop& loop) noexcept;

    // See uv_tcp_bind
    std::expected<void, Error> bind(const SocketAddress& address, uv_tcp_flags flags) noexcept;

    // See uv_tcp_getsockname
    std::expected<SocketAddress, Error> getSockName() const noexcept;

private:
    TCPServer() noexcept = default;
};

}  // namespace uv
}  // namespace wrapper
}  // namespace scaler

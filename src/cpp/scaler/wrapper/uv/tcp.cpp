#include "scaler/wrapper/uv/tcp.h"

#include <cassert>
#include <cstring>

namespace scaler {
namespace wrapper {
namespace uv {

namespace details {

std::expected<SocketAddress, Error> getSockName(const uv_tcp_t& handle) noexcept
{
    sockaddr_storage storage {};
    int len = sizeof(storage);

    const int err = uv_tcp_getsockname(&handle, reinterpret_cast<sockaddr*>(&storage), &len);
    if (err) {
        return std::unexpected(Error {err});
    }

    return SocketAddress::fromSockAddr(reinterpret_cast<sockaddr*>(&storage));
}

}  // namespace details

std::expected<TCPSocket, Error> TCPSocket::init(Loop& loop) noexcept
{
    TCPSocket socket {};

    const int err = uv_tcp_init(&loop.native(), &socket.handle().native());
    if (err) {
        return std::unexpected {Error {err}};
    }

    return socket;
}

std::expected<ConnectRequest, Error> TCPSocket::connect(const SocketAddress& address, ConnectCallback callback) noexcept
{
    ConnectRequest request([callback = std::move(callback)](int status) mutable {
        if (status < 0) {
            callback(std::unexpected {Error {status}});
        } else {
            callback({});
        }
    });

    const int err =
        uv_tcp_connect(&request.native(), &handle().native(), address.toSockAddr(), &ConnectRequest::onCallback);
    if (err) {
        request.release();
        return std::unexpected(Error {err});
    }

    return request;
}

std::expected<void, Error> TCPSocket::closeReset() noexcept
{
    const int err = uv_tcp_close_reset(&handle().native(), &Handle<uv_tcp_t, ReadCallback>::free);
    if (err) {
        return std::unexpected(Error {err});
    }

    // Prevent the handle to be closed a second time through RAII.
    handle().release();

    return {};
}

std::expected<SocketAddress, Error> TCPSocket::getSockName() const noexcept
{
    return details::getSockName(handle().native());
}

std::expected<SocketAddress, Error> TCPSocket::getPeerName() const noexcept
{
    sockaddr_storage storage {};
    int len = sizeof(storage);

    const int err = uv_tcp_getpeername(&handle().native(), reinterpret_cast<sockaddr*>(&storage), &len);
    if (err) {
        return std::unexpected(Error {err});
    }

    return SocketAddress::fromSockAddr(reinterpret_cast<sockaddr*>(&storage));
}

std::expected<void, Error> TCPSocket::nodelay(bool enable) noexcept
{
    const int err = uv_tcp_nodelay(&handle().native(), enable);
    if (err) {
        return std::unexpected(Error {err});
    }

    return {};
}

std::expected<TCPServer, Error> TCPServer::init(Loop& loop) noexcept
{
    TCPServer server {};

    const int err = uv_tcp_init(&loop.native(), &server.handle().native());
    if (err) {
        return std::unexpected {Error {err}};
    }

    return server;
}

std::expected<void, Error> TCPServer::bind(const SocketAddress& address, uv_tcp_flags flags) noexcept
{
    const int err = uv_tcp_bind(&handle().native(), address.toSockAddr(), flags);
    if (err) {
        return std::unexpected(Error {err});
    }

    return {};
}

std::expected<SocketAddress, Error> TCPServer::getSockName() const noexcept
{
    return details::getSockName(handle().native());
}

}  // namespace uv
}  // namespace wrapper
}  // namespace scaler

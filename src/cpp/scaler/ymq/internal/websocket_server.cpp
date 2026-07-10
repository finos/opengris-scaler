#include "scaler/ymq/internal/websocket_server.h"

#include <utility>

namespace scaler {
namespace ymq {
namespace internal {

std::expected<WebSocketServer, scaler::wrapper::uv::Error> WebSocketServer::init(
    scaler::wrapper::uv::Loop& loop) noexcept
{
    std::expected<scaler::wrapper::uv::TCPServer, scaler::wrapper::uv::Error> server =
        scaler::wrapper::uv::TCPServer::init(loop);
    if (!server.has_value()) {
        return std::unexpected {server.error()};
    }

    return WebSocketServer {std::move(server.value())};
}

WebSocketServer::WebSocketServer(scaler::wrapper::uv::TCPServer server) noexcept: _server(std::move(server))
{
}

std::expected<void, scaler::wrapper::uv::Error> WebSocketServer::bind(
    const WebSocketAddress& address, uv_tcp_flags flags) noexcept
{
    _address = address;
    return _server.bind(address.tcpAddress, flags);
}

std::expected<void, scaler::wrapper::uv::Error> WebSocketServer::listen(
    int backlog, scaler::wrapper::uv::ConnectionCallback callback) noexcept
{
    return _server.listen(backlog, std::move(callback));
}

std::expected<void, scaler::wrapper::uv::Error> WebSocketServer::accept(
    WebSocketStream& connection, WebSocketStream::HandshakeDoneCallback callback) noexcept
{
    std::expected<void, scaler::wrapper::uv::Error> acceptResult = _server.accept(connection.transport());
    if (!acceptResult.has_value()) {
        return acceptResult;
    }

    return connection.accept(std::move(callback));
}

std::expected<WebSocketAddress, scaler::wrapper::uv::Error> WebSocketServer::getSockName() const noexcept
{
    if (!_address.has_value()) {
        // getSockName() called before bind()
        return std::unexpected {scaler::wrapper::uv::Error {UV_EINVAL}};
    }

    std::expected<scaler::wrapper::uv::SocketAddress, scaler::wrapper::uv::Error> tcpAddress = _server.getSockName();
    if (!tcpAddress.has_value()) {
        return std::unexpected {tcpAddress.error()};
    }

    // Reconstruct the WebSocket address with the actual bound port (handles port 0 auto-assignment).
    WebSocketAddress address = _address.value();
    address.tcpAddress       = tcpAddress.value();
    address.port             = static_cast<uint16_t>(tcpAddress->port());
    return address;
}

}  // namespace internal
}  // namespace ymq
}  // namespace scaler

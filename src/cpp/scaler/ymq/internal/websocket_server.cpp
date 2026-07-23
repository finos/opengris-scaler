#include "scaler/ymq/internal/websocket_server.h"

#include <utility>
#include <variant>

namespace scaler {
namespace ymq {
namespace internal {

std::expected<WebSocketServer, scaler::wrapper::uv::Error> WebSocketServer::init(
    scaler::wrapper::uv::Loop& loop, bool secure) noexcept
{
    if (secure) {
        auto secureServer = scaler::wrapper::openssl::SecureServer::init(loop);
        if (!secureServer.has_value()) {
            return std::unexpected {secureServer.error()};
        }
        return WebSocketServer {std::move(secureServer.value())};
    } else {
        auto tcpServer = scaler::wrapper::uv::TCPServer::init(loop);
        if (!tcpServer.has_value()) {
            return std::unexpected {tcpServer.error()};
        }
        return WebSocketServer {std::move(tcpServer.value())};
    }
}

WebSocketServer::WebSocketServer(Server server) noexcept: _server(std::move(server))
{
}

std::expected<void, scaler::wrapper::uv::Error> WebSocketServer::bind(
    const WebSocketAddress& address, uv_tcp_flags flags) noexcept
{
    _address = address;
    return std::visit([&](auto& server) { return server.bind(address.tcpAddress, flags); }, _server);
}

std::expected<void, scaler::wrapper::uv::Error> WebSocketServer::listen(
    int backlog, scaler::wrapper::uv::ConnectionCallback callback) noexcept
{
    return std::visit([&](auto& server) { return server.listen(backlog, std::move(callback)); }, _server);
}

std::expected<void, scaler::wrapper::uv::Error> WebSocketServer::accept(
    WebSocketStream& connection, WebSocketStream::HandshakeDoneCallback callback) noexcept
{
    if (auto* secureServer = std::get_if<scaler::wrapper::openssl::SecureServer>(&_server)) {
        auto* secureSocket = std::get_if<scaler::wrapper::openssl::SecureSocket>(&connection.transport());
        if (secureSocket == nullptr) {
            return std::unexpected {scaler::wrapper::uv::Error {UV_EINVAL}};
        }

        // The WebSocket HTTP Upgrade exchange must not start until the TLS handshake has completed.
        scaler::wrapper::uv::ConnectCallback onHandshakeDone =
            [&connection,
             callback = std::move(callback)](std::expected<void, scaler::wrapper::uv::Error> handshakeResult) mutable {
                if (!handshakeResult.has_value()) {
                    callback(std::unexpected {handshakeResult.error()});
                    return;
                }

                connection.accept(std::move(callback));
            };

        return secureServer->accept(*secureSocket, std::move(onHandshakeDone));
    } else {
        auto& tcpServer = std::get<scaler::wrapper::uv::TCPServer>(_server);
        auto* tcpSocket = std::get_if<scaler::wrapper::uv::TCPSocket>(&connection.transport());
        if (tcpSocket == nullptr) {
            return std::unexpected {scaler::wrapper::uv::Error {UV_EINVAL}};
        }

        std::expected<void, scaler::wrapper::uv::Error> acceptResult = tcpServer.accept(*tcpSocket);
        if (!acceptResult.has_value()) {
            return acceptResult;
        }

        return connection.accept(std::move(callback));
    }
}

std::expected<WebSocketAddress, scaler::wrapper::uv::Error> WebSocketServer::getSockName() const noexcept
{
    if (!_address.has_value()) {
        // getSockName() called before bind()
        return std::unexpected {scaler::wrapper::uv::Error {UV_EINVAL}};
    }

    auto tcpAddress = std::visit([](const auto& server) { return server.getSockName(); }, _server);
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

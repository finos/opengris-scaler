#include "scaler/wrapper/openssl/secure_server.h"

#include <utility>

namespace scaler {
namespace wrapper {
namespace openssl {

std::expected<SecureServer, uv::Error> SecureServer::init(uv::Loop& loop) noexcept
{
    std::expected<uv::TCPServer, uv::Error> server = uv::TCPServer::init(loop);
    if (!server.has_value()) {
        return std::unexpected {server.error()};
    }

    return SecureServer {std::move(server.value())};
}

SecureServer::SecureServer(uv::TCPServer server) noexcept: _server(std::move(server))
{
}

std::expected<void, uv::Error> SecureServer::bind(const uv::SocketAddress& address, uv_tcp_flags flags) noexcept
{
    return _server.bind(address, flags);
}

std::expected<void, uv::Error> SecureServer::listen(int backlog, uv::ConnectionCallback callback) noexcept
{
    return _server.listen(backlog, std::move(callback));
}

std::expected<void, uv::Error> SecureServer::accept(
    SecureSocket& connection, uv::ConnectCallback onHandshakeDone) noexcept
{
    std::expected<void, uv::Error> acceptResult = _server.accept(connection.transport());
    if (!acceptResult.has_value()) {
        return acceptResult;
    }

    return connection.accept(std::move(onHandshakeDone));
}

std::expected<uv::SocketAddress, uv::Error> SecureServer::getSockName() const noexcept
{
    return _server.getSockName();
}

}  // namespace openssl
}  // namespace wrapper
}  // namespace scaler

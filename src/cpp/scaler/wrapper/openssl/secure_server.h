#pragma once

#include <expected>

#include "scaler/wrapper/openssl/secure_socket.h"
#include "scaler/wrapper/openssl/ssl_context.h"
#include "scaler/wrapper/uv/callback.h"
#include "scaler/wrapper/uv/loop.h"
#include "scaler/wrapper/uv/socket_address.h"
#include "scaler/wrapper/uv/tcp.h"

namespace scaler {
namespace wrapper {
namespace openssl {

// A libuv-like server socket implementing SSL/TLS using OpenSSL.
class SecureServer {
public:
    static std::expected<SecureServer, uv::Error> init(uv::Loop& loop) noexcept;

    std::expected<void, uv::Error> bind(const uv::SocketAddress& address, uv_tcp_flags flags) noexcept;

    std::expected<void, uv::Error> listen(int backlog, uv::ConnectionCallback callback) noexcept;

    std::expected<void, uv::Error> accept(
        SecureSocket& connection, uv::ConnectCallback onHandshakeDone = [](std::expected<void, uv::Error>) {}) noexcept;

    std::expected<uv::SocketAddress, uv::Error> getSockName() const noexcept;

private:
    SecureServer(uv::TCPServer server) noexcept;

    uv::TCPServer _server;
};

}  // namespace openssl
}  // namespace wrapper
}  // namespace scaler

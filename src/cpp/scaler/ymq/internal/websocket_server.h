#pragma once

#include <expected>
#include <optional>
#include <variant>

#include "scaler/wrapper/openssl/secure_server.h"
#include "scaler/wrapper/uv/callback.h"
#include "scaler/wrapper/uv/error.h"
#include "scaler/wrapper/uv/loop.h"
#include "scaler/wrapper/uv/tcp.h"
#include "scaler/ymq/address.h"
#include "scaler/ymq/internal/websocket_stream.h"

namespace scaler {
namespace ymq {
namespace internal {

// A libuv-like server socket implementing the WebSocket protocol.
class WebSocketServer {
public:
    static std::expected<WebSocketServer, scaler::wrapper::uv::Error> init(
        scaler::wrapper::uv::Loop& loop, bool secure = false) noexcept;

    std::expected<void, scaler::wrapper::uv::Error> bind(const WebSocketAddress& address, uv_tcp_flags flags) noexcept;

    std::expected<void, scaler::wrapper::uv::Error> listen(
        int backlog, scaler::wrapper::uv::ConnectionCallback callback) noexcept;

    std::expected<void, scaler::wrapper::uv::Error> accept(
        WebSocketStream& connection, WebSocketStream::HandshakeDoneCallback callback) noexcept;

    std::expected<WebSocketAddress, scaler::wrapper::uv::Error> getSockName() const noexcept;

private:
    using Server = std::variant<scaler::wrapper::uv::TCPServer, scaler::wrapper::openssl::SecureServer>;

    WebSocketServer(Server server) noexcept;

    Server _server;

    std::optional<WebSocketAddress> _address {};
};

}  // namespace internal
}  // namespace ymq
}  // namespace scaler

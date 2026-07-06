#pragma once

#include <cstdint>
#include <expected>
#include <span>
#include <variant>

#include "scaler/wrapper/openssl/secure_socket.h"
#include "scaler/wrapper/uv/callback.h"
#include "scaler/wrapper/uv/error.h"
#include "scaler/wrapper/uv/pipe.h"
#include "scaler/wrapper/uv/tcp.h"
#include "scaler/ymq/internal/websocket_stream.h"

namespace scaler {
namespace ymq {
namespace internal {

// A connected client socket abstracting TCP, TLS, IPC, and WebSocket transports.
class Client {
public:
    explicit Client(scaler::wrapper::uv::TCPSocket socket) noexcept;
    explicit Client(scaler::wrapper::openssl::SecureSocket socket) noexcept;
    explicit Client(scaler::wrapper::uv::Pipe pipe) noexcept;
    explicit Client(WebSocketStream stream) noexcept;

    ~Client() noexcept = default;

    Client(const Client&)            = delete;
    Client& operator=(const Client&) = delete;

    Client(Client&&) noexcept            = default;
    Client& operator=(Client&&) noexcept = default;

    bool isTCP() const noexcept;

    bool isSecure() const noexcept;

    bool isWebSocket() const noexcept;

    // The buffers' content must remain valid until the callback is called.
    std::expected<void, scaler::wrapper::uv::Error> write(
        std::span<const std::span<const uint8_t>> buffers, scaler::wrapper::uv::WriteCallback callback) noexcept;

    std::expected<void, scaler::wrapper::uv::Error> readStart(scaler::wrapper::uv::ReadCallback callback) noexcept;

    void readStop() noexcept;

    std::expected<void, scaler::wrapper::uv::Error> setNoDelay(bool enable) noexcept;

    std::expected<void, scaler::wrapper::uv::Error> shutdown(scaler::wrapper::uv::ShutdownCallback callback) noexcept;

    // Send a RST packet to the remote, immediately closing the connection.
    //
    // Returns UV_ENOTSUP if not supported.
    std::expected<void, scaler::wrapper::uv::Error> closeReset() noexcept;

private:
    std::variant<
        scaler::wrapper::uv::TCPSocket,
        scaler::wrapper::openssl::SecureSocket,
        scaler::wrapper::uv::Pipe,
        WebSocketStream>
        _socket;
};

}  // namespace internal
}  // namespace ymq
}  // namespace scaler

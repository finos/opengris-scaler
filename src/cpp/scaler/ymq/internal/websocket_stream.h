#pragma once

#include <cstdint>
#include <expected>
#include <memory>
#include <span>
#include <vector>

#include "scaler/utility/move_only_function.h"
#include "scaler/wrapper/uv/callback.h"
#include "scaler/wrapper/uv/error.h"
#include "scaler/wrapper/uv/tcp.h"
#include "scaler/ymq/address.h"

namespace scaler {
namespace ymq {
namespace internal {

struct ClientUpgradeContext;
struct ServerUpgradeContext;

// Manages the WebSocket protocol over an established TCP connection.
//
// Handles the RFC 6455 HTTP/1.1 Upgrade handshake and binary frame framing/deframing.
// YMQ magic bytes and message frames are transported as WebSocket binary frames.
// Only ws:// (plain TCP) is supported; wss:// (TLS) requires an external library.
class WebSocketStream {
public:
    // Perform the server-side HTTP/1.1 Upgrade handshake on an accepted TCP socket,
    // then call callback with a ready-to-use WebSocketStream.
    static void upgradeAsServer(
        scaler::wrapper::uv::TCPSocket socket,
        scaler::utility::MoveOnlyFunction<void(std::expected<WebSocketStream, scaler::wrapper::uv::Error>)>
            callback) noexcept;

    // Perform the client-side HTTP/1.1 Upgrade handshake on a connected TCP socket,
    // then call callback with a ready-to-use WebSocketStream.
    static void upgradeAsClient(
        scaler::wrapper::uv::TCPSocket socket,
        const WebSocketAddress& address,
        scaler::utility::MoveOnlyFunction<void(std::expected<WebSocketStream, scaler::wrapper::uv::Error>)>
            callback) noexcept;

    ~WebSocketStream() noexcept = default;

    WebSocketStream(const WebSocketStream&)            = delete;
    WebSocketStream& operator=(const WebSocketStream&) = delete;

    WebSocketStream(WebSocketStream&&) noexcept            = default;
    WebSocketStream& operator=(WebSocketStream&&) noexcept = default;

    // The buffers' content must remain valid until the callback is called.
    std::expected<void, scaler::wrapper::uv::Error> write(
        std::span<const std::span<const uint8_t>> buffers, scaler::wrapper::uv::WriteCallback callback) noexcept;

    std::expected<void, scaler::wrapper::uv::Error> readStart(scaler::wrapper::uv::ReadCallback callback) noexcept;

    void readStop() noexcept;

    std::expected<void, scaler::wrapper::uv::Error> shutdown(scaler::wrapper::uv::ShutdownCallback callback) noexcept;

    std::expected<void, scaler::wrapper::uv::Error> closeReset() noexcept;

private:
    static WebSocketStream fromUpgradedSocket(
        scaler::wrapper::uv::TCPSocket socket, bool isServer, std::vector<uint8_t> leftover = {}) noexcept;

    static void finishClientUpgrade(std::shared_ptr<ClientUpgradeContext> ctx) noexcept;
    static void finishServerUpgrade(std::shared_ptr<ServerUpgradeContext> ctx) noexcept;

    struct State {
        scaler::wrapper::uv::TCPSocket _socket;
        bool _isServer;
        std::vector<uint8_t> _recvBuffer {};
        std::vector<uint8_t> _fragmentBuffer {};
        bool _readActive {false};
        scaler::wrapper::uv::ReadCallback _readCallback {};

        State(scaler::wrapper::uv::TCPSocket socket, bool isServer) noexcept;
    };

    explicit WebSocketStream(std::shared_ptr<State> state) noexcept;

    static void onRead(
        std::shared_ptr<State> state,
        std::expected<std::span<const uint8_t>, scaler::wrapper::uv::Error> result) noexcept;

    std::shared_ptr<State> _state;
};

}  // namespace internal
}  // namespace ymq
}  // namespace scaler

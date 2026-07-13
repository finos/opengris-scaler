#pragma once

#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "scaler/utility/move_only_function.h"
#include "scaler/wrapper/uv/callback.h"
#include "scaler/wrapper/uv/error.h"
#include "scaler/wrapper/uv/loop.h"
#include "scaler/wrapper/uv/request.h"
#include "scaler/wrapper/uv/tcp.h"
#include "scaler/ymq/address.h"

namespace scaler {
namespace ymq {
namespace internal {

// Manages the WebSocket protocol over a TCP connection.
//
// Handles the RFC 6455 HTTP/1.1 Upgrade handshake and binary frame framing/deframing.
// YMQ magic bytes and message frames are transported as WebSocket binary frames.
// Only ws:// (plain TCP) is supported; wss:// (TLS) requires an external library.
class WebSocketStream {
public:
    using HandshakeDoneCallback =
        scaler::utility::MoveOnlyFunction<void(std::expected<void, scaler::wrapper::uv::Error>)>;

    static std::expected<WebSocketStream, scaler::wrapper::uv::Error> init(scaler::wrapper::uv::Loop& loop) noexcept;

    ~WebSocketStream() noexcept;

    WebSocketStream(const WebSocketStream&)            = delete;
    WebSocketStream& operator=(const WebSocketStream&) = delete;

    WebSocketStream(WebSocketStream&&) noexcept            = default;
    WebSocketStream& operator=(WebSocketStream&&) noexcept = default;

    // Perform the client-side HTTP/1.1 Upgrade handshake.
    //
    // The callback is called when the upgrade completes (or fails).
    std::expected<scaler::wrapper::uv::ConnectRequest, scaler::wrapper::uv::Error> connect(
        WebSocketAddress address, HandshakeDoneCallback callback) noexcept;

    // Perform the server-side HTTP/1.1 Upgrade handshake.
    //
    // The callback is called when the upgrade completes (or fails).
    std::expected<void, scaler::wrapper::uv::Error> accept(HandshakeDoneCallback callback) noexcept;

    // Returns the underlying TCP socket.
    scaler::wrapper::uv::TCPSocket& transport() noexcept;

    // The buffers' content must remain valid until the callback is called.
    std::expected<void, scaler::wrapper::uv::Error> write(
        std::span<const std::span<const uint8_t>> buffers, scaler::wrapper::uv::WriteCallback callback) noexcept;

    std::expected<void, scaler::wrapper::uv::Error> readStart(scaler::wrapper::uv::ReadCallback callback) noexcept;

    void readStop() noexcept;

    std::expected<void, scaler::wrapper::uv::Error> shutdown(scaler::wrapper::uv::ShutdownCallback callback) noexcept;

    std::expected<void, scaler::wrapper::uv::Error> closeReset() noexcept;

private:
    enum class Role { Undefined, Client, Server };

    struct State {
        scaler::wrapper::uv::TCPSocket _socket;
        Role _role {Role::Undefined};

        // Used only during the HTTP Upgrade handshake, reset once completed.
        std::optional<HandshakeDoneCallback> _upgradeCallback {};

        std::vector<uint8_t> _recvBuffer {};
        std::vector<uint8_t> _fragmentBuffer {};
        bool _readActive {false};
        scaler::wrapper::uv::ReadCallback _readCallback {};

        explicit State(scaler::wrapper::uv::TCPSocket socket) noexcept;
    };

    explicit WebSocketStream(std::shared_ptr<State> state) noexcept;

    static void upgradeAsClient(
        std::shared_ptr<State> state,
        WebSocketAddress address,
        std::expected<void, scaler::wrapper::uv::Error> result) noexcept;

    static std::expected<void, scaler::wrapper::uv::Error> upgradeAsServer(std::shared_ptr<State> state) noexcept;

    static void finishClientUpgrade(std::shared_ptr<State> state, std::string key) noexcept;

    static void finishServerUpgrade(std::shared_ptr<State> state) noexcept;

    // Invokes and clears the upgrade callback exactly.
    static void completeUpgrade(
        const std::shared_ptr<State>& state, std::expected<void, scaler::wrapper::uv::Error> result) noexcept;

    static void onRead(
        std::shared_ptr<State> state,
        std::expected<std::span<const uint8_t>, scaler::wrapper::uv::Error> result) noexcept;

    static void processRecvBuffer(std::shared_ptr<State> state) noexcept;

    std::shared_ptr<State> _state;
};

}  // namespace internal
}  // namespace ymq
}  // namespace scaler

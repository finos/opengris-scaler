#pragma once

#include <cstdint>
#include <expected>
#include <optional>
#include <queue>
#include <span>

#include "scaler/logging/logging.h"
#include "scaler/utility/move_only_function.h"
#include "scaler/uv_ymq/typedefs.h"
#include "scaler/wrapper/uv/callback.h"
#include "scaler/wrapper/uv/error.h"
#include "scaler/wrapper/uv/loop.h"
#include "scaler/ymq/bytes.h"

namespace scaler {
namespace uv_ymq {
namespace internal {

// A bidirectional message connection with message buffering, automatic identity exchange and reconnect capability.
//
// The connection allows "offline" use: send operations can be queued while the connection isn't established yet. The
// connection is established by calling connect(), and remains established until disconnect() is called or until a
// remote disconnect event it triggered.
//
// Disconnected connections can be re-established by calling connect() again after a disconnect event.
class MessageConnection {
public:
    enum class State {
        Disconnected,  // connect() hasn't been called yet, or the connection has been disconnected.
        Connected,     // The network link is established, but the identity handshake hasn't completed yet.
        Established,   // The network link is established and the identity handshake has completed.
    };

    enum class DisconnectReason {
        // Disconnected because the connection dropped unexpectedly.
        Aborted,
        // Disconnected because the remote explicitly requested it (graceful disconnect).
        // The remote does not expect a reconnect attempt.
        Disconnected,
    };

    using RemoteIdentityCallback = scaler::utility::MoveOnlyFunction<void(Identity)>;

    using RemoteDisconnectCallback = scaler::utility::MoveOnlyFunction<void(DisconnectReason)>;

    using SendMessageCallback = scaler::utility::MoveOnlyFunction<void(std::expected<void, scaler::ymq::Error>)>;

    using RecvMessageCallback = scaler::utility::MoveOnlyFunction<void(scaler::ymq::Bytes)>;

    MessageConnection(
        scaler::wrapper::uv::Loop& loop,
        Identity localIdentity,
        std::optional<Identity> remoteIdentity,
        RemoteIdentityCallback onRemoteIdentityCallback,
        RemoteDisconnectCallback onRemoteDisconnectCallback,
        RecvMessageCallback onRecvMessageCallback) noexcept;

    ~MessageConnection() noexcept;

    MessageConnection(MessageConnection&&) noexcept            = default;
    MessageConnection& operator=(MessageConnection&&) noexcept = default;

    MessageConnection(const MessageConnection&) noexcept            = delete;
    MessageConnection& operator=(const MessageConnection&) noexcept = delete;

    State state() const noexcept;

    // Returns true if a client is connected (network link is live).
    bool connected() const noexcept;

    // Returns true if the connection is established (network link is live and identity exchange completed).
    bool established() const noexcept;

    void connect(Client client) noexcept;

    void disconnect() noexcept;

    // Same as disconnect(), but send a RST packet to the remote, triggering an Aborted event.
    //
    // Only TCP clients are supported.
    void abort() noexcept;

    const Identity& localIdentity() const noexcept;

    // Return nullopt if the remote entity isn't know because the identity handshake hasn't completed yet.
    const std::optional<Identity>& remoteIdentity() const noexcept;

    // Send a message to the remote, invoking the callback when the message has been sent.
    //
    // If the connection is not established yet, the message is queued and sent once the connection is established.
    //
    // If the connection disconnects, the message will be queued again until the connection is re-established.
    void sendMessage(scaler::ymq::Bytes messagePayload, SendMessageCallback onMessageSent) noexcept;

private:
    static constexpr size_t HEADER_SIZE = sizeof(uint64_t);

    struct SendOperation {
        scaler::ymq::Bytes _messagePayload;
        SendMessageCallback _onMessageSent;

        // This is strictly equal to _messagePayload.size(), but we need a dereferenceable and stable memory location
        // for that value while doing the write().
        uint64_t _messageSize;
    };

    struct RecvOperation {
        size_t _cursor {0};
        uint64_t _header {0};
        scaler::ymq::Bytes _messagePayload {};
    };

    scaler::ymq::Logger _logger {};

    State _state {State::Disconnected};

    scaler::wrapper::uv::Loop& _loop;

    Identity _localIdentity;
    std::optional<Identity> _remoteIdentity;

    RemoteIdentityCallback _onRemoteIdentityCallback;
    RemoteDisconnectCallback _onRemoteDisconnectCallback;
    RecvMessageCallback _onRecvMessageCallback;

    std::optional<Client> _client {};

    // Messages not yet submitted to the remote.
    std::queue<SendOperation> _sendPending {};

    // The current partially received message being assembled.
    RecvOperation _recvCurrent {};

    void reinitialize() noexcept;

    static void onWriteDone(
        SendMessageCallback callback, std::expected<void, scaler::wrapper::uv::Error> result) noexcept;

    void onRead(std::expected<std::span<const uint8_t>, scaler::wrapper::uv::Error> result) noexcept;

    void onMessage(scaler::ymq::Bytes messagePayload) noexcept;

    void onRemoteIdentity(scaler::ymq::Bytes messagePayload) noexcept;

    void onRemoteDisconnect(DisconnectReason reason) noexcept;

    void sendLocalIdentity() noexcept;

    void processSendQueue() noexcept;

    void write(std::span<const std::span<const uint8_t>> buffers, scaler::wrapper::uv::WriteCallback callback) noexcept;

    void setNoDelay() noexcept;

    void readStart() noexcept;

    void readStop() noexcept;

    size_t readHeader(std::span<const uint8_t> data) noexcept;

    size_t readMessage(std::span<const uint8_t> data) noexcept;

    bool allocateMessage() noexcept;
};

}  // namespace internal
}  // namespace uv_ymq
}  // namespace scaler

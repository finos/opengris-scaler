#pragma once

#include <cstdint>
#include <expected>
#include <optional>
#include <queue>
#include <span>
#include <vector>

#include "scaler/logging/logging.h"
#include "scaler/utility/move_only_function.h"
#include "scaler/wrapper/uv/callback.h"
#include "scaler/wrapper/uv/error.h"
#include "scaler/ymq/bytes.h"
#include "scaler/ymq/typedefs.h"

namespace scaler {
namespace ymq {
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

    using SendMessageCallback = scaler::utility::MoveOnlyFunction<void(std::expected<void, Error>)>;

    using RecvMessageCallback = scaler::utility::MoveOnlyFunction<void(Bytes)>;

    MessageConnection(
        Identity localIdentity,
        std::optional<Identity> remoteIdentity,
        RemoteIdentityCallback onRemoteIdentityCallback,
        RemoteDisconnectCallback onRemoteDisconnectCallback,
        RecvMessageCallback onRecvMessageCallback) noexcept;

    ~MessageConnection() noexcept;

    MessageConnection(MessageConnection&&) noexcept            = delete;
    MessageConnection& operator=(MessageConnection&&) noexcept = delete;

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
    void sendMessage(Bytes messagePayload, SendMessageCallback onMessageSent) noexcept;

private:
    using Header = uint64_t;

    using SendCallback = scaler::utility::MoveOnlyFunction<void(std::expected<void, Error>)>;

    using RecvCallback = scaler::utility::MoveOnlyFunction<void(Bytes)>;

    struct SendOperation {
        std::vector<std::span<const uint8_t>> _buffers;

        SendCallback _onSendDone;
    };

    struct RecvOperation {
        Bytes _buffer {};
        size_t _cursor {0};

        RecvCallback _onRecvDone {};
    };

    Logger _logger {};

    State _state {State::Disconnected};

    Identity _localIdentity;
    std::optional<Identity> _remoteIdentity;

    RemoteIdentityCallback _onRemoteIdentityCallback;
    RemoteDisconnectCallback _onRemoteDisconnectCallback;
    RecvMessageCallback _onRecvMessageCallback;

    std::optional<Client> _client {};

    // Sent buffers not yet submitted to the remote.
    std::queue<SendOperation> _sendPending {};

    // The current partially received receive buffer being assembled.
    RecvOperation _recvCurrent {};

    void shutdownClient() noexcept;

    void initialize() noexcept;

    // Sends the buffers.
    //
    // Buffers' memory must remain valid until the callback is called.
    void send(std::vector<std::span<const uint8_t>> buffers, SendCallback callback) noexcept;

    // Receives a buffer of exactly the given size.
    void recv(size_t size, RecvCallback result) noexcept;

    void sendHandshake() noexcept;

    void recvMagicNumber() noexcept;

    void recvMessage() noexcept;

    static void onWriteDone(
        SendMessageCallback callback, std::expected<void, scaler::wrapper::uv::Error> result) noexcept;

    void onRead(std::expected<std::span<const uint8_t>, scaler::wrapper::uv::Error> result) noexcept;

    void onRemoteIdentity(Bytes payload) noexcept;

    void onRemoteDisconnect(DisconnectReason reason) noexcept;

    void processSendQueue() noexcept;

    void processSendOperation(SendOperation operation) noexcept;

    void write(std::span<const std::span<const uint8_t>> buffers, scaler::wrapper::uv::WriteCallback callback) noexcept;

    void setNoDelay() noexcept;

    void readStart() noexcept;

    void readStop() noexcept;
};

}  // namespace internal
}  // namespace ymq
}  // namespace scaler

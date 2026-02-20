#pragma once

#include <chrono>
#include <expected>
#include <memory>
#include <optional>
#include <queue>
#include <string>

#include "scaler/error/error.h"
#include "scaler/utility/move_only_function.h"
#include "scaler/uv_ymq/address.h"
#include "scaler/uv_ymq/configuration.h"
#include "scaler/uv_ymq/internal/connect_client.h"
#include "scaler/uv_ymq/internal/event_loop_thread.h"
#include "scaler/uv_ymq/internal/message_connection.h"
#include "scaler/uv_ymq/io_context.h"
#include "scaler/uv_ymq/typedefs.h"
#include "scaler/ymq/message.h"

namespace scaler {
namespace uv_ymq {

// A socket that connects to a remote address and exchanges messages with a single remote peer.
//
// On unexpected disconnection, the socket will automatically try to reconnect to the remote address.
//
// Thread-safe: all operations are scheduled onto the socket's event loop thread.
class ConnectorSocket {
public:
    using ConnectCallback = scaler::utility::MoveOnlyFunction<void(std::expected<void, scaler::ymq::Error>)>;

    using SendMessageCallback = scaler::utility::MoveOnlyFunction<void(std::expected<void, scaler::ymq::Error>)>;

    using RecvMessageCallback =
        scaler::utility::MoveOnlyFunction<void(std::expected<scaler::ymq::Message, scaler::ymq::Error>)>;

    // Create a connector socket and initiate connection to the remote address.
    //
    // The socket will automatically retry connection up to maxRetryTimes on failure.
    // The onConnectCallback will be invoked once the connection succeeds or all retries are exhausted.
    ConnectorSocket(
        IOContext& context,
        Identity identity,
        std::string address,
        ConnectCallback onConnectCallback,
        size_t maxRetryTimes                     = defaultClientMaxRetryTimes,
        std::chrono::milliseconds initRetryDelay = defaultClientInitRetryDelay) noexcept;

    ~ConnectorSocket() noexcept;

    ConnectorSocket(const ConnectorSocket&)            = delete;
    ConnectorSocket& operator=(const ConnectorSocket&) = delete;

    ConnectorSocket(ConnectorSocket&&) noexcept            = default;
    ConnectorSocket& operator=(ConnectorSocket&&) noexcept = default;

    const Identity& identity() const noexcept;

    // Send a message to the connected remote peer.
    //
    // If not yet connected, the message will be queued and sent once the connection is established.
    void sendMessage(scaler::ymq::Bytes messagePayload, SendMessageCallback onMessageSent) noexcept;

    // Receive a message from the connected remote peer.
    void recvMessage(RecvMessageCallback onRecvMessage) noexcept;

private:
    struct State {
        internal::EventLoopThread& _thread;

        const Identity _identity;

        Address _remoteAddress;
        size_t _maxRetryTimes;
        std::chrono::milliseconds _initRetryDelay;

        std::optional<internal::ConnectClient> _connectClient {};

        std::optional<internal::MessageConnection> _connection {};

        std::queue<RecvMessageCallback> _pendingRecvCallbacks {};
        std::queue<scaler::ymq::Message> _pendingRecvMessages {};

        bool _disconnected {false};

        State(
            internal::EventLoopThread& thread,
            Identity identity,
            Address remoteAddress,
            size_t maxRetryTimes,
            std::chrono::milliseconds initRetryDelay) noexcept
            : _thread(thread)
            , _identity(std::move(identity))
            , _remoteAddress(std::move(remoteAddress))
            , _maxRetryTimes(maxRetryTimes)
            , _initRetryDelay(initRetryDelay)
        {
        }
    };

    std::shared_ptr<State> _state;

    static void connect(std::shared_ptr<State> state, ConnectCallback onConnectCallback) noexcept;

    static void onClientConnected(
        std::shared_ptr<State> state,
        ConnectCallback onConnectCallback,
        Address parsedAddress,
        std::expected<Client, scaler::ymq::Error> result) noexcept;

    static void onRemoteDisconnect(
        std::shared_ptr<State> state, internal::MessageConnection::DisconnectReason reason) noexcept;

    static void onMessage(std::shared_ptr<State> state, scaler::ymq::Bytes messagePayload) noexcept;

    static void fillPendingRecvCallbacksWithErr(std::shared_ptr<State> state, scaler::ymq::Error err) noexcept;
};

}  // namespace uv_ymq
}  // namespace scaler

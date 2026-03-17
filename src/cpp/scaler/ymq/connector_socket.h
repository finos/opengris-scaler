#pragma once

#include <chrono>
#include <expected>
#include <memory>
#include <optional>
#include <queue>
#include <string>

#include "scaler/error/error.h"
#include "scaler/utility/move_only_function.h"
#include "scaler/ymq/address.h"
#include "scaler/ymq/configuration.h"
#include "scaler/ymq/internal/accept_server.h"
#include "scaler/ymq/internal/connect_client.h"
#include "scaler/ymq/internal/event_loop_thread.h"
#include "scaler/ymq/internal/message_connection.h"
#include "scaler/ymq/io_context.h"
#include "scaler/ymq/message.h"
#include "scaler/ymq/typedefs.h"

namespace scaler {
namespace ymq {

// A socket that exchanges messages with a single remote peer.
//
// It can either actively connect to a remote binder socket or binding connector (connect()), or bind to a local address
// and accept a single incoming connection (bind()).
//
// Thread-safe: all operations are scheduled onto the socket's event loop thread.
class ConnectorSocket {
public:
    using ConnectCallback = scaler::utility::MoveOnlyFunction<void(std::expected<void, Error>)>;

    using BindCallback = scaler::utility::MoveOnlyFunction<void(std::expected<Address, Error>)>;

    using ShutdownCallback = scaler::utility::MoveOnlyFunction<void()>;

    using SendMessageCallback = scaler::utility::MoveOnlyFunction<void(std::expected<void, Error>)>;

    using RecvMessageCallback = scaler::utility::MoveOnlyFunction<void(std::expected<Message, Error>)>;

    // Create a connector socket and initiate connection to the remote address.
    //
    // The socket will automatically retry connecting to the remote address up to maxRetryTimes on failure.
    //
    // The onConnectCallback will be invoked once the connection succeeds or all retries are exhausted.
    static ConnectorSocket connect(
        IOContext& context,
        Identity identity,
        std::string address,
        ConnectCallback onConnectCallback,
        size_t maxRetryTimes                     = defaultClientMaxRetryTimes,
        std::chrono::milliseconds initRetryDelay = defaultClientInitRetryDelay) noexcept;

    // Create a connector socket that binds to a local address and waits for a single incoming connection.
    //
    // If the remote unexpectedly disconnect, the socket will wait for reconnection.
    //
    // The onBindCallback will be invoked once the server is listening, or with an error.
    static ConnectorSocket bind(
        IOContext& context, Identity identity, std::string address, BindCallback onBindCallback) noexcept;

    ~ConnectorSocket() noexcept;

    ConnectorSocket(const ConnectorSocket&)            = delete;
    ConnectorSocket& operator=(const ConnectorSocket&) = delete;

    ConnectorSocket(ConnectorSocket&&) noexcept            = default;
    ConnectorSocket& operator=(ConnectorSocket&&) noexcept = default;

    // Terminate this socket and its connection.
    void shutdown(ShutdownCallback onShutdownCallback) noexcept;

    const Identity& identity() const noexcept;

    // Send a message to the connected remote peer.
    //
    // If not yet connected, the message will be queued and sent once the connection is established.
    void sendMessage(Bytes messagePayload, SendMessageCallback onMessageSent) noexcept;

    // Receive a message from the connected remote peer.
    void recvMessage(RecvMessageCallback onRecvMessage) noexcept;

private:
    struct State {
        internal::EventLoopThread& _thread;

        const Identity _identity;
        Address _address;

        bool _isBinding;

        bool _disconnected {false};

        // Connect mode fields
        size_t _maxRetryTimes {0};
        std::chrono::milliseconds _initRetryDelay {0};
        std::optional<internal::ConnectClient> _connectClient {};

        // Bind mode fields
        std::optional<internal::AcceptServer> _acceptServer {};

        // Common fields
        std::optional<internal::MessageConnection> _connection {};
        std::queue<RecvMessageCallback> _pendingRecvCallbacks {};
        std::queue<Message> _pendingRecvMessages {};

        State(internal::EventLoopThread& thread, Identity identity, Address address, bool isBinding) noexcept
            : _thread(thread), _identity(std::move(identity)), _address(std::move(address)), _isBinding(isBinding)
        {
        }
    };

    ConnectorSocket() noexcept = default;

    std::shared_ptr<State> _state;

    static void tryConnect(std::shared_ptr<State> state, ConnectCallback onConnectCallback) noexcept;

    static void onClientConnected(
        std::shared_ptr<State> state,
        ConnectCallback onConnectCallback,
        Address parsedAddress,
        std::expected<Client, Error> result) noexcept;

    static void onClientAccepted(std::shared_ptr<State> state, Client client) noexcept;

    static void onRemoteDisconnect(
        std::shared_ptr<State> state, internal::MessageConnection::DisconnectReason reason) noexcept;

    static void onMessage(std::shared_ptr<State> state, Bytes messagePayload) noexcept;

    static void emplaceMessageConnection(std::shared_ptr<State> state) noexcept;

    static void fillPendingRecvCallbacksWithErr(std::shared_ptr<State> state, Error err) noexcept;
};

}  // namespace ymq
}  // namespace scaler

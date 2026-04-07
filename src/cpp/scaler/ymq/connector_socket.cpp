#include "scaler/ymq/connector_socket.h"

#include <cassert>
#include <functional>
#include <utility>

namespace scaler {
namespace ymq {

ConnectorSocket ConnectorSocket::connect(
    IOContext& context,
    Identity identity,
    std::string address,
    ConnectCallback onConnectCallback,
    size_t maxRetryTimes,
    std::chrono::milliseconds initRetryDelay) noexcept
{
    internal::EventLoopThread& thread = context.nextThread();

    auto parsedAddress = Address::fromString(address);
    if (!parsedAddress.has_value()) {
        onConnectCallback(std::unexpected(parsedAddress.error()));
        return {};
    }

    ConnectorSocket socket;
    socket._state                  = std::make_shared<State>(thread, std::move(identity), parsedAddress.value(), false);
    socket._state->_maxRetryTimes  = maxRetryTimes;
    socket._state->_initRetryDelay = initRetryDelay;

    socket._state->_thread.executeThreadSafe(
        [state = socket._state, onConnectCallback = std::move(onConnectCallback)]() mutable {
            emplaceMessageConnection(state);

            tryConnect(state, std::move(onConnectCallback));
        });

    return socket;
}

ConnectorSocket ConnectorSocket::bind(
    IOContext& context, Identity identity, std::string address, BindCallback onBindCallback) noexcept
{
    internal::EventLoopThread& thread = context.nextThread();

    auto parsedAddress = Address::fromString(address);
    if (!parsedAddress.has_value()) {
        onBindCallback(std::unexpected(parsedAddress.error()));
        return {};
    }

    ConnectorSocket socket;
    socket._state = std::make_shared<State>(thread, std::move(identity), parsedAddress.value(), true);

    socket._state->_thread.executeThreadSafe(
        [state = socket._state, onBindCallback = std::move(onBindCallback)]() mutable {
            emplaceMessageConnection(state);

            state->_acceptServer.emplace(
                state->_thread.loop(), state->_address, std::bind_front(&ConnectorSocket::onClientAccepted, state));

            Address boundAddress = state->_acceptServer->address();
            onBindCallback(boundAddress);
        });

    return socket;
}

ConnectorSocket::~ConnectorSocket() noexcept
{
    shutdown([]() {});
}

void ConnectorSocket::shutdown(ShutdownCallback onShutdownCallback) noexcept
{
    if (_state == nullptr) {
        onShutdownCallback();
        return;  // instance moved
    }

    _state->_thread.executeThreadSafe([state = _state, onShutdownCallback = std::move(onShutdownCallback)]() mutable {
        state->_connectClient.reset();
        state->_acceptServer.reset();

        state->_connection.reset();

        // Fail all pending receive callbacks
        fillPendingRecvCallbacksWithErr(state, Error::ErrorCode::SocketStopRequested);

        state->_pendingRecvMessages = {};

        onShutdownCallback();
    });

    _state = nullptr;
}

const Identity& ConnectorSocket::identity() const noexcept
{
    return _state->_identity;
}

void ConnectorSocket::sendMessage(Bytes messagePayload, SendMessageCallback onMessageSent) noexcept
{
    _state->_thread.executeThreadSafe([state          = _state,
                                       messagePayload = std::move(messagePayload),
                                       onMessageSent  = std::move(onMessageSent)]() mutable {
        if (state->_disconnected) {
            onMessageSent(std::unexpected {Error::ErrorCode::ConnectorSocketClosedByRemoteEnd});
            return;
        }
        state->_connection->sendMessage(std::move(messagePayload), std::move(onMessageSent));
    });
}

void ConnectorSocket::recvMessage(RecvMessageCallback onRecvMessage) noexcept
{
    _state->_thread.executeThreadSafe([state = _state, onRecvMessage = std::move(onRecvMessage)]() mutable {
        if (state->_disconnected) {
            onRecvMessage(std::unexpected {Error::ErrorCode::ConnectorSocketClosedByRemoteEnd});
            return;
        }

        if (!state->_pendingRecvMessages.empty()) {
            // There is a message ready, call the callback immediately
            Message message = std::move(state->_pendingRecvMessages.front());
            state->_pendingRecvMessages.pop();
            onRecvMessage(std::move(message));
            return;
        }

        // No messages are pending, queue the callback until a message arrives
        state->_pendingRecvCallbacks.push(std::move(onRecvMessage));
    });
}

void ConnectorSocket::tryConnect(std::shared_ptr<State> state, ConnectCallback onConnectCallback) noexcept
{
    assert(!state->_isBinding);
    assert(!state->_connectClient.has_value() && "tryConnect() called while already connecting");

    state->_connectClient = internal::ConnectClient {
        state->_thread.loop(),
        state->_address,
        [state, onConnectCallback = std::move(onConnectCallback), address = state->_address](
            std::expected<internal::Client, Error> result) mutable {
            ConnectorSocket::onClientConnected(
                std::move(state), std::move(onConnectCallback), std::move(address), std::move(result));
        },
        state->_maxRetryTimes,
        state->_initRetryDelay};
}

void ConnectorSocket::onClientConnected(
    std::shared_ptr<State> state,
    ConnectCallback onConnectCallback,
    [[maybe_unused]] Address address,
    std::expected<internal::Client, Error> result) noexcept
{
    assert(!state->_isBinding);

    // The ConnectClient is no longer needed
    state->_connectClient.reset();

    if (!result.has_value()) {
        state->_disconnected = true;
        onConnectCallback(std::unexpected {result.error()});
        fillPendingRecvCallbacksWithErr(state, result.error());
        return;
    }

    // Connection succeeded, now link it to the MessageConnection
    state->_connection->connect(std::move(result.value()));

    // Notify the user the connection succeeded
    onConnectCallback({});
}

void ConnectorSocket::onClientAccepted(std::shared_ptr<State> state, internal::Client client) noexcept
{
    assert(state->_isBinding);

    if (state->_disconnected) {
        // Socket was gracefully shut down, ignore new connections
        return;
    }

    // Establish the connection with the new client. Possibly replacing any existing client
    state->_connection->connect(std::move(client));
}

void ConnectorSocket::onRemoteDisconnect(
    std::shared_ptr<State> state, internal::MessageConnection::DisconnectReason reason) noexcept
{
    if (reason == internal::MessageConnection::DisconnectReason::Disconnected) {
        // Remote end disconnected gracefully - mark as permanently disconnected
        state->_disconnected = true;
        fillPendingRecvCallbacksWithErr(state, Error::ErrorCode::ConnectorSocketClosedByRemoteEnd);
    } else if (!state->_isBinding) {
        // Connection aborted (e.g. network error) while in connect mode - retry connection
        tryConnect(state, [](std::expected<void, Error>) {});
    }
}

void ConnectorSocket::onMessage(std::shared_ptr<State> state, Bytes messagePayload) noexcept
{
    assert(state->_connection->remoteIdentity().has_value());

    Message message;
    message.address = Bytes {state->_connection->remoteIdentity().value()};
    message.payload = std::move(messagePayload);

    if (state->_pendingRecvCallbacks.empty()) {
        // No callback waiting, buffer the message until the user calls recvMessage()
        state->_pendingRecvMessages.push(std::move(message));
        return;
    }

    RecvMessageCallback onRecvMessage = std::move(state->_pendingRecvCallbacks.front());
    state->_pendingRecvCallbacks.pop();
    onRecvMessage(std::move(message));
}

void ConnectorSocket::emplaceMessageConnection(std::shared_ptr<State> state) noexcept
{
    state->_connection = std::make_unique<internal::MessageConnection>(
        state->_identity,
        std::nullopt,
        [](Identity) {},
        std::bind_front(&ConnectorSocket::onRemoteDisconnect, state),
        std::bind_front(&ConnectorSocket::onMessage, state));
}

void ConnectorSocket::fillPendingRecvCallbacksWithErr(std::shared_ptr<State> state, Error err) noexcept
{
    while (!state->_pendingRecvCallbacks.empty()) {
        auto callback = std::move(state->_pendingRecvCallbacks.front());
        state->_pendingRecvCallbacks.pop();
        callback(std::unexpected(err));
    }
}

}  // namespace ymq
}  // namespace scaler

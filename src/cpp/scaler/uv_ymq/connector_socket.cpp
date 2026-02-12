#include "scaler/uv_ymq/connector_socket.h"

#include <cassert>
#include <functional>
#include <utility>

namespace scaler {
namespace uv_ymq {

ConnectorSocket::ConnectorSocket(
    IOContext& context,
    Identity identity,
    std::string address,
    ConnectCallback onConnectCallback,
    size_t maxRetryTimes,
    std::chrono::milliseconds initRetryDelay) noexcept
{
    EventLoopThread& thread = context.nextThread();

    auto parsedAddress = Address::fromString(address);
    if (!parsedAddress.has_value()) {
        onConnectCallback(std::unexpected(parsedAddress.error()));
        return;
    }

    _state = std::make_shared<State>(thread, std::move(identity), parsedAddress.value(), maxRetryTimes, initRetryDelay);

    _state->_thread.executeThreadSafe([state = _state, onConnectCallback = std::move(onConnectCallback)]() mutable {
        state->_connection.emplace(
            MessageConnection {
                state->_thread.loop(),
                state->_identity,
                std::nullopt,
                [](Identity) {},
                std::bind_front(&ConnectorSocket::onRemoteDisconnect, state),
                std::bind_front(&ConnectorSocket::onMessage, state)});

        connect(state, std::move(onConnectCallback));
    });
}

ConnectorSocket::~ConnectorSocket() noexcept
{
    if (_state == nullptr) {
        return;  // instance moved
    }

    _state->_thread.executeThreadSafe([state = _state]() {
        // Disconnect the client
        state->_connectClient.reset();
        state->_connection.reset();

        // Fail all pending receive callbacks
        fillPendingRecvCallbacksWithErr(state, scaler::ymq::Error::ErrorCode::IOSocketStopRequested);

        state->_pendingRecvMessages = {};
    });
}

const Identity& ConnectorSocket::identity() const noexcept
{
    return _state->_identity;
}

void ConnectorSocket::sendMessage(scaler::ymq::Bytes messagePayload, SendMessageCallback onMessageSent) noexcept
{
    _state->_thread.executeThreadSafe([state          = _state,
                                       messagePayload = std::move(messagePayload),
                                       onMessageSent  = std::move(onMessageSent)]() mutable {
        if (state->_disconnected) {
            onMessageSent(std::unexpected(scaler::ymq::Error::ErrorCode::ConnectorSocketClosedByRemoteEnd));
            return;
        }
        state->_connection->sendMessage(std::move(messagePayload), std::move(onMessageSent));
    });
}

void ConnectorSocket::recvMessage(RecvMessageCallback onRecvMessage) noexcept
{
    _state->_thread.executeThreadSafe([state = _state, onRecvMessage = std::move(onRecvMessage)]() mutable {
        if (state->_disconnected) {
            onRecvMessage(std::unexpected(scaler::ymq::Error::ErrorCode::ConnectorSocketClosedByRemoteEnd));
            return;
        }

        if (!state->_pendingRecvMessages.empty()) {
            // There is a message ready, call the callback immediately
            scaler::ymq::Message message = std::move(state->_pendingRecvMessages.front());
            state->_pendingRecvMessages.pop();
            onRecvMessage(std::move(message));
            return;
        }

        // No messages are pending, queue the callback until a message arrives
        state->_pendingRecvCallbacks.push(std::move(onRecvMessage));
    });
}

void ConnectorSocket::connect(std::shared_ptr<State> state, ConnectCallback onConnectCallback) noexcept
{
    assert(!state->_connectClient.has_value() && "connect() called while already connecting");

    state->_connectClient = ConnectClient {
        state->_thread.loop(),
        state->_remoteAddress,
        [state, onConnectCallback = std::move(onConnectCallback), remoteAddress = state->_remoteAddress](
            std::expected<Client, scaler::ymq::Error> result) mutable {
            ConnectorSocket::onClientConnected(
                std::move(state), std::move(onConnectCallback), std::move(remoteAddress), std::move(result));
        },
        state->_maxRetryTimes,
        state->_initRetryDelay};
}

void ConnectorSocket::onClientConnected(
    std::shared_ptr<State> state,
    ConnectCallback onConnectCallback,
    Address address,
    std::expected<Client, scaler::ymq::Error> result) noexcept
{
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

void ConnectorSocket::onRemoteDisconnect(
    std::shared_ptr<State> state, MessageConnection::DisconnectReason reason) noexcept
{
    if (reason == MessageConnection::DisconnectReason::Disconnected) {
        // Remote end disconnected gracefully - mark as permanently disconnected
        state->_disconnected = true;
        fillPendingRecvCallbacksWithErr(state, scaler::ymq::Error::ErrorCode::ConnectorSocketClosedByRemoteEnd);
    } else {
        // Connection aborted (e.g., network error) - retry connection
        connect(state, [](std::expected<void, scaler::ymq::Error>) {});
    }
}

void ConnectorSocket::onMessage(std::shared_ptr<State> state, scaler::ymq::Bytes messagePayload) noexcept
{
    assert(state->_connection->remoteIdentity().has_value());

    scaler::ymq::Message message;
    message.address = scaler::ymq::Bytes(state->_connection->remoteIdentity().value());
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

void ConnectorSocket::fillPendingRecvCallbacksWithErr(std::shared_ptr<State> state, scaler::ymq::Error err) noexcept
{
    while (!state->_pendingRecvCallbacks.empty()) {
        auto callback = std::move(state->_pendingRecvCallbacks.front());
        state->_pendingRecvCallbacks.pop();
        callback(std::unexpected(err));
    }
}

}  // namespace uv_ymq
}  // namespace scaler

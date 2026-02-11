#include "scaler/uv_ymq/binder_socket.h"

#include <cassert>
#include <functional>
#include <utility>

namespace scaler {
namespace uv_ymq {

BinderSocket::BinderSocket(IOContext& context, Identity identity) noexcept
{
    EventLoopThread& thread = context.nextThread();
    _state                  = std::make_shared<State>(thread, std::move(identity));
}

BinderSocket::~BinderSocket() noexcept
{
    _state->_thread.executeThreadSafe([state = _state]() {
        // Disconnect all servers
        state->_servers.clear();

        // Disconnect all connections
        state->_connections.clear();
        state->_identityToConnectionID.clear();

        // Fail all pending receive callbacks
        while (!state->_pendingRecvCallbacks.empty()) {
            auto callback = std::move(state->_pendingRecvCallbacks.front());
            state->_pendingRecvCallbacks.pop();
            callback(std::unexpected(scaler::ymq::Error(scaler::ymq::Error::ErrorCode::IOSocketStopRequested)));
        }

        // Clear all pending messages
        state->_pendingSendMessages.clear();
        state->_pendingRecvMessages = {};
    });
}

const Identity& BinderSocket::identity() const noexcept
{
    return _state->_identity;
}

void BinderSocket::bindTo(std::string address, BindCallback onBindCallback) noexcept
{
    _state->_thread.executeThreadSafe(
        [state = _state, address = std::move(address), callback = std::move(onBindCallback)]() mutable {
            auto parsedAddress = Address::fromString(address);
            if (!parsedAddress.has_value()) {
                callback(std::unexpected(parsedAddress.error()));
                return;
            }

            state->_servers.emplace_back(
                state->_thread.loop(), parsedAddress.value(), std::bind_front(&BinderSocket::onClientConnect, state));

            // Get the actual bound address (useful when binding to port 0)
            Address boundAddress = state->_servers.back().address();
            callback(boundAddress);
        });
}

void BinderSocket::sendMessage(
    Identity remoteIdentity, scaler::ymq::Bytes messagePayload, SendMessageCallback onMessageSent) noexcept
{
    _state->_thread.executeThreadSafe([state          = _state,
                                       remoteIdentity = std::move(remoteIdentity),
                                       messagePayload = std::move(messagePayload),
                                       callback       = std::move(onMessageSent)]() mutable {
        auto it = state->_identityToConnectionID.find(remoteIdentity);
        if (it == state->_identityToConnectionID.end()) {
            // We don't know this identity yet, queue the message until the peer eventually connects
            state->_pendingSendMessages[remoteIdentity].emplace_back(
                PendingSendMessage {std::move(messagePayload), std::move(callback)});
            return;
        }

        MessageConnection& connection = state->_connections.at(it->second);
        connection.sendMessage(std::move(messagePayload), std::move(callback));
    });
}

void BinderSocket::recvMessage(RecvMessageCallback onRecvMessage) noexcept
{
    _state->_thread.executeThreadSafe([state = _state, onRecvMessage = std::move(onRecvMessage)]() mutable {
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

void BinderSocket::closeConnection(Identity remoteIdentity) noexcept
{
    _state->_thread.executeThreadSafe([state = _state, remoteIdentity = std::move(remoteIdentity)]() {
        auto node = state->_identityToConnectionID.extract(remoteIdentity);
        if (node.empty()) {
            // Connection not found. Might have disconnected earlier.
            return;
        }
        ConnectionID connectionID = node.mapped();

        state->_connections.erase(connectionID);
    });
}

void BinderSocket::onClientConnect(std::shared_ptr<State> state, Client client) noexcept
{
    MessageConnection& connection = createConnection(state, std::nullopt);
    connection.connect(std::move(client));
}

void BinderSocket::onRemoteIdentity(
    std::shared_ptr<State> state, ConnectionID connectionId, Identity remoteIdentity) noexcept
{
    if (state->_identityToConnectionID.contains(remoteIdentity)) {
        // Another connection already established to this remote. Disconnect and destroy the old one.
        state->_connections.erase(state->_identityToConnectionID[remoteIdentity]);
    }

    state->_identityToConnectionID[remoteIdentity] = connectionId;

    // Send any pending messages previously queued for this identity
    auto pendingIt = state->_pendingSendMessages.find(remoteIdentity);
    if (pendingIt != state->_pendingSendMessages.end()) {
        MessageConnection& connection = state->_connections.at(connectionId);
        for (auto& pending: pendingIt->second) {
            connection.sendMessage(std::move(pending.messagePayload), std::move(pending.onMessageSent));
        }
        state->_pendingSendMessages.erase(pendingIt);
    }
}

void BinderSocket::onRemoteDisconnect(
    std::shared_ptr<State> state, ConnectionID connectionId, MessageConnection::DisconnectReason /*reason*/) noexcept
{
    auto node = state->_connections.extract(connectionId);
    assert(!node.empty());

    MessageConnection& connection = node.mapped();
    if (connection.remoteIdentity()) {
        state->_identityToConnectionID.erase(connection.remoteIdentity().value());
    }
}

void BinderSocket::onMessage(
    std::shared_ptr<State> state, ConnectionID connectionId, scaler::ymq::Bytes messagePayload) noexcept
{
    MessageConnection& connection = state->_connections.at(connectionId);
    assert(connection.remoteIdentity().has_value());

    scaler::ymq::Message message;
    message.address = scaler::ymq::Bytes(connection.remoteIdentity().value());
    message.payload = std::move(messagePayload);

    if (state->_pendingRecvCallbacks.empty()) {
        // No callback waiting, buffer the message until the user calls recvMessage()
        state->_pendingRecvMessages.push(std::move(message));
        return;
    }

    RecvMessageCallback callback = std::move(state->_pendingRecvCallbacks.front());
    state->_pendingRecvCallbacks.pop();
    callback(std::move(message));
}

MessageConnection& BinderSocket::createConnection(
    std::shared_ptr<State> state, std::optional<Identity> remoteIdentity) noexcept
{
    ConnectionID connectionId = state->_connectionCounter++;

    MessageConnection connection(
        state->_thread.loop(),
        state->_identity,
        remoteIdentity,
        std::bind_front(&BinderSocket::onRemoteIdentity, state, connectionId),
        std::bind_front(&BinderSocket::onRemoteDisconnect, state, connectionId),
        std::bind_front(&BinderSocket::onMessage, state, connectionId));

    auto [it, inserted] = state->_connections.emplace(connectionId, std::move(connection));

    return it->second;
}

}  // namespace uv_ymq
}  // namespace scaler

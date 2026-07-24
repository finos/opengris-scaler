#include "scaler/ymq/binder_socket.h"

#include <cassert>
#include <chrono>
#include <functional>
#include <utility>

#include "scaler/ymq/buffered_bytes.h"
#include "scaler/ymq/configuration.h"

namespace scaler {
namespace ymq {

BinderSocket::BinderSocket(IOContext& context, Identity identity) noexcept
{
    internal::EventLoopThread& thread = context.nextThread();
    _state                            = std::make_shared<State>(thread, std::move(identity));
}

BinderSocket::~BinderSocket() noexcept
{
    shutdown([]() {});
}

void BinderSocket::shutdown(ShutdownCallback onShutdownCallback) noexcept
{
    if (_state == nullptr) {
        onShutdownCallback();
        return;  // instance moved
    }

    _state->_thread.executeThreadSafe([state = _state, onShutdownCallback = std::move(onShutdownCallback)]() mutable {
        // Disconnect all servers
        state->_servers.clear();

        // Disconnect all connections
        state->_connections.clear();
        state->_identityToConnectionID.clear();

        // Fail all pending receive callbacks
        while (!state->_pendingRecvCallbacks.empty()) {
            auto callback = std::move(state->_pendingRecvCallbacks.front());
            state->_pendingRecvCallbacks.pop();
            callback(std::unexpected {Error {Error::ErrorCode::SocketStopRequested}});
        }

        // Fail all pending send callbacks
        for (auto& [_, pendingMessages]: state->_pendingSendMessages) {
            for (auto& pendingMessage: pendingMessages) {
                pendingMessage.onMessageSent(
                    std::unexpected {Error {Error::ErrorCode::SocketStopRequested}},
                    std::move(pendingMessage.messagePayload));
            }
        }
        state->_pendingSendMessages.clear();
        state->_pendingRecvMessages = {};

        onShutdownCallback();
    });

    _state = nullptr;
}

const Identity& BinderSocket::identity() const noexcept
{
    return _state->_identity;
}

void BinderSocket::bindTo(std::string address, BindCallback onBindCallback, std::optional<TLSConfig> tlsConfig) noexcept
{
    _state->_thread.executeThreadSafe([state     = _state,
                                       address   = std::move(address),
                                       callback  = std::move(onBindCallback),
                                       tlsConfig = std::move(tlsConfig)]() mutable {
        auto parsedAddress = Address::fromString(address, std::move(tlsConfig));
        if (!parsedAddress.has_value()) {
            callback(std::unexpected(parsedAddress.error()));
            return;
        }

        auto server = internal::AcceptServer::init(
            state->_thread.loop(), parsedAddress.value(), std::bind_front(&BinderSocket::onClientConnect, state));
        if (!server.has_value()) {
            callback(std::unexpected {std::move(server.error())});
            return;
        }

        state->_servers.push_back(std::move(server.value()));

        // Get the actual bound address (useful when binding to port 0)
        Address boundAddress = state->_servers.back().address();
        callback(boundAddress);
    });
}

void BinderSocket::sendMessage(
    Identity remoteIdentity, std::unique_ptr<Bytes> messagePayload, SendMessageCallback onMessageSent) noexcept
{
    _state->_thread.executeThreadSafe([state          = _state,
                                       remoteIdentity = std::move(remoteIdentity),
                                       messagePayload = std::move(messagePayload),
                                       callback       = std::move(onMessageSent)]() mutable {
        auto it = state->_identityToConnectionID.find(remoteIdentity);
        if (it == state->_identityToConnectionID.end()) {
            // The peer is not currently connected.
            //
            // If we have seen this identity complete an identity exchange and then disconnect,
            // the peer is gone and won't be back; fail the callback immediately. Otherwise this
            // is a send-before-first-connect case (used by some tests and the warm-up path);
            // queue until the peer eventually identifies itself.
            if (state->_disconnectedIdentities.count(remoteIdentity) > 0) {
                callback(
                    std::unexpected {Error {Error::ErrorCode::ConnectorSocketClosedByRemoteEnd}},
                    std::move(messagePayload));
                return;
            }

            state->_pendingSendMessages[remoteIdentity].emplace_back(
                PendingSendMessage {std::move(messagePayload), std::move(callback)});
            return;
        }

        internal::MessageConnection& connection = *state->_connections.at(it->second);
        connection.sendMessage(std::move(messagePayload), std::move(callback));
    });
}

void BinderSocket::sendMulticastMessage(
    std::unique_ptr<Bytes> messagePayload, std::optional<Identity> remotePrefix) noexcept
{
    _state->_thread.executeThreadSafe(
        [state = _state, messagePayload = std::move(messagePayload), remotePrefix = std::move(remotePrefix)]() mutable {
            for (const auto& [_, connectionPtr]: state->_connections) {
                if (remotePrefix.has_value()) {
                    const std::optional<Identity>& remoteIdentity = connectionPtr->remoteIdentity();
                    if (!remoteIdentity.has_value() || !remoteIdentity->starts_with(remotePrefix.value())) {
                        continue;
                    }
                }

                connectionPtr->sendMessage(
                    std::make_unique<BufferedBytes>(
                        reinterpret_cast<const char*>(messagePayload->data()), messagePayload->size()),
                    []([[maybe_unused]] std::expected<void, Error> result,
                       [[maybe_unused]] std::unique_ptr<Bytes>) noexcept {});
            }
        });
}

void BinderSocket::recvMessage(RecvMessageCallback onRecvMessage) noexcept
{
    _state->_thread.executeThreadSafe([state = _state, onRecvMessage = std::move(onRecvMessage)]() mutable {
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

void BinderSocket::closeConnection(Identity remoteIdentity) noexcept
{
    _state->_thread.executeThreadSafe([state = _state, remoteIdentity = std::move(remoteIdentity)]() mutable {
        auto it = state->_identityToConnectionID.find(remoteIdentity);
        if (it == state->_identityToConnectionID.end()) {
            // Connection not found. Might have disconnected earlier.
            return;
        }
        ConnectionID connectionID = it->second;

        // Reuse the disconnect path: it extracts the connection from _connections, erases the
        // identity mapping, populates _disconnectedIdentities, and drains _pendingSendMessages.
        onRemoteDisconnect(std::move(state), connectionID, internal::MessageConnection::DisconnectReason::Disconnected);
    });
}

void BinderSocket::onClientConnect(std::shared_ptr<State> state, internal::Client client) noexcept
{
    internal::MessageConnection& connection = createConnection(state, std::nullopt);
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
    state->_disconnectedIdentities.erase(remoteIdentity);  // peer is back; allow sends to queue again

    // Send any pending messages previously queued for this identity
    auto pendingIt = state->_pendingSendMessages.find(remoteIdentity);
    if (pendingIt != state->_pendingSendMessages.end()) {
        internal::MessageConnection& connection = *state->_connections.at(connectionId);
        for (auto& pending: pendingIt->second) {
            connection.sendMessage(std::move(pending.messagePayload), std::move(pending.onMessageSent));
        }
        state->_pendingSendMessages.erase(pendingIt);
    }
}

void BinderSocket::onRemoteDisconnect(
    std::shared_ptr<State> state,
    ConnectionID connectionId,
    [[maybe_unused]] internal::MessageConnection::DisconnectReason reason) noexcept
{
    auto node = state->_connections.extract(connectionId);
    assert(!node.empty());

    internal::MessageConnection& connection = *node.mapped();
    if (!connection.remoteIdentity()) {
        return;
    }

    const Identity& remoteIdentity = connection.remoteIdentity().value();
    state->_identityToConnectionID.erase(remoteIdentity);

    // Treat BOTH a graceful close (Disconnected, a clean FIN) and an abrupt drop (Aborted, a reset
    // or timeout) as terminal. A binder peer that is gone -- e.g. an evicted pod whose connection
    // reset -- will not come back to drain a queued send, so recording it below makes a later send
    // fail fast (ConnectorSocketClosedByRemoteEnd) instead of queueing forever in
    // _pendingSendMessages and hanging the caller's coroutine -- which, on the scheduler's receive
    // loop, freezes the whole event loop (it stops echoing heartbeats and never recovers). A peer
    // that does reconnect with the same identity clears this entry in onRemoteIdentity and resumes;
    // sends issued during the gap fail and the caller reroutes or drops them.
    //
    // The set is bounded by a TTL purge driven from here (insert-time) so it cannot grow without
    // bound even under workloads that churn many short-lived peers (e.g. nested-task clients).
    const auto now = std::chrono::steady_clock::now();
    purgeExpiredDisconnectedIdentities(*state, now);
    state->_disconnectedIdentities[remoteIdentity] = now;
    state->_disconnectedIdentityInsertions.emplace_back(now, remoteIdentity);

    // Drain any sends already queued for this identity (rare: covers the case where Python
    // raced ahead of the disconnect).
    auto pendingIt = state->_pendingSendMessages.find(remoteIdentity);
    if (pendingIt != state->_pendingSendMessages.end()) {
        for (auto& pending: pendingIt->second) {
            pending.onMessageSent(
                std::unexpected {Error {Error::ErrorCode::ConnectorSocketClosedByRemoteEnd}},
                std::move(pending.messagePayload));
        }
        state->_pendingSendMessages.erase(pendingIt);
    }
}

void BinderSocket::onMessage(
    std::shared_ptr<State> state, ConnectionID connectionId, std::unique_ptr<Bytes> messagePayload) noexcept
{
    internal::MessageConnection& connection = *state->_connections.at(connectionId);
    assert(connection.remoteIdentity().has_value());

    Message message;
    message.address = std::make_unique<BufferedBytes>(connection.remoteIdentity().value());
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

internal::MessageConnection& BinderSocket::createConnection(
    std::shared_ptr<State> state, std::optional<Identity> remoteIdentity) noexcept
{
    ConnectionID connectionId = state->_connectionCounter++;

    auto connection = std::make_unique<internal::MessageConnection>(
        state->_identity,
        remoteIdentity,
        std::bind_front(&BinderSocket::onRemoteIdentity, state, connectionId),
        std::bind_front(&BinderSocket::onRemoteDisconnect, state, connectionId),
        std::bind_front(&BinderSocket::onMessage, state, connectionId));

    auto [it, inserted] = state->_connections.emplace(connectionId, std::move(connection));

    return *it->second;
}

void BinderSocket::purgeExpiredDisconnectedIdentities(State& state, std::chrono::steady_clock::time_point now) noexcept
{
    while (!state._disconnectedIdentityInsertions.empty() &&
           now - state._disconnectedIdentityInsertions.front().first > disconnectedIdentityTTL) {
        const auto& [expiredTs, expiredId] = state._disconnectedIdentityInsertions.front();
        // Only drop the map entry if its timestamp matches; otherwise the peer re-disconnected
        // since this deque slot was written and the map holds a fresher entry that we must keep.
        if (auto it = state._disconnectedIdentities.find(expiredId);
            it != state._disconnectedIdentities.end() && it->second == expiredTs) {
            state._disconnectedIdentities.erase(it);
        }
        state._disconnectedIdentityInsertions.pop_front();
    }
}

}  // namespace ymq
}  // namespace scaler

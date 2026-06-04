#pragma once

#include <chrono>
#include <cstdint>
#include <deque>
#include <expected>
#include <map>
#include <memory>
#include <optional>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "scaler/error/error.h"
#include "scaler/utility/move_only_function.h"
#include "scaler/ymq/address.h"
#include "scaler/ymq/internal/accept_server.h"
#include "scaler/ymq/internal/event_loop_thread.h"
#include "scaler/ymq/internal/message_connection.h"
#include "scaler/ymq/io_context.h"
#include "scaler/ymq/message.h"
#include "scaler/ymq/typedefs.h"

namespace scaler {
namespace ymq {

// A socket that binds to a local address and accepts messages from multiple remote peers.
//
// Thread-safe: all operations are scheduled onto the socket's event loop thread.
class BinderSocket {
public:
    using ShutdownCallback = scaler::utility::MoveOnlyFunction<void()>;

    using BindCallback = scaler::utility::MoveOnlyFunction<void(std::expected<Address, Error>)>;

    using SendMessageCallback =
        scaler::utility::MoveOnlyFunction<void(std::expected<void, Error>, std::unique_ptr<Bytes>)>;

    using RecvMessageCallback = scaler::utility::MoveOnlyFunction<void(std::expected<Message, Error>)>;

    BinderSocket(IOContext& context, Identity identity) noexcept;

    ~BinderSocket() noexcept;

    BinderSocket(const BinderSocket&)            = delete;
    BinderSocket& operator=(const BinderSocket&) = delete;

    BinderSocket(BinderSocket&&) noexcept            = default;
    BinderSocket& operator=(BinderSocket&&) noexcept = default;

    // Terminate this socket and all its connections.
    void shutdown(ShutdownCallback onShutdownCallback) noexcept;

    const Identity& identity() const noexcept;

    // Bind to a TCP or IPC address string (e.g. "tcp://127.0.0.1:9000", "ipc://my_socket").
    //
    // Multiple bind calls are allowed: the socket will accept connections on all bound addresses.
    void bindTo(std::string address, BindCallback onBindCallback) noexcept;

    // Send a message to a remote identity.
    void sendMessage(
        Identity remoteIdentity, std::unique_ptr<Bytes> messagePayload, SendMessageCallback onMessageSent) noexcept;

    // Send a message to multiple currently connected peers.
    //
    // This method is "fire-and-forget" and always succeeds.
    //
    // If remotePrefix is provided, only peers whose identity starts with the prefix will receive the message.
    void sendMulticastMessage(
        std::unique_ptr<Bytes> messagePayload, std::optional<Identity> remotePrefix = std::nullopt) noexcept;

    // Receive a message from any remote identity.
    void recvMessage(RecvMessageCallback onRecvMessage) noexcept;

    // Close a connection to a remote identity.
    //
    // Do nothing if the connection does not exist.
    void closeConnection(Identity remoteIdentity) noexcept;

private:
    // Assign a unique, internal, ID to connections.
    //
    // This allows fast retrieval even when these don't have a remote identity yet.
    using ConnectionID = uint64_t;

    struct PendingSendMessage {
        std::unique_ptr<Bytes> messagePayload;
        SendMessageCallback onMessageSent;
    };

    struct State {
        internal::EventLoopThread& _thread;

        const Identity _identity;

        // Support binding to multiple addresses (TCP and/or IPC)
        std::vector<internal::AcceptServer> _servers {};

        ConnectionID _connectionCounter {0};

        std::map<ConnectionID, std::unique_ptr<internal::MessageConnection>> _connections {};
        std::map<Identity, ConnectionID> _identityToConnectionID {};

        // Identities that completed identity exchange and then disconnected, mapped to the time
        // of the most recent disconnect. Sends to these are failed immediately (instead of
        // queued in _pendingSendMessages) to avoid a hang when the peer has gracefully gone
        // away. Cleared on reconnect or by TTL expiry.
        //
        // The TTL only needs to bracket the worst-case lag between libuv processing the disconnect
        // (which adds the entry) and the Python layer catching up on messages libuv buffered from
        // that peer (which may try to reply). That lag is millisecond-scale in practice; we keep a
        // generous 60s window. _disconnectedIdentityInsertions holds entries in insertion order so
        // the expiry sweep is O(expired) and not O(size); each entry stores its own timestamp so a
        // peer that re-disconnects (mapped to a fresh time) is not evicted by an older deque entry.
        std::map<Identity, std::chrono::steady_clock::time_point> _disconnectedIdentities {};
        std::deque<std::pair<std::chrono::steady_clock::time_point, Identity>> _disconnectedIdentityInsertions {};

        std::map<Identity, std::vector<PendingSendMessage>> _pendingSendMessages {};

        std::queue<RecvMessageCallback> _pendingRecvCallbacks {};
        std::queue<Message> _pendingRecvMessages {};

        State(internal::EventLoopThread& thread, Identity identity) noexcept
            : _thread(thread), _identity(std::move(identity))
        {
        }
    };

    std::shared_ptr<State> _state;

    static void onClientConnect(std::shared_ptr<State> state, internal::Client client) noexcept;

    static void onRemoteIdentity(
        std::shared_ptr<State> state, ConnectionID connectionId, Identity remoteIdentity) noexcept;

    static void onRemoteDisconnect(
        std::shared_ptr<State> state,
        ConnectionID connectionId,
        internal::MessageConnection::DisconnectReason reason) noexcept;

    static void onMessage(
        std::shared_ptr<State> state, ConnectionID connectionId, std::unique_ptr<Bytes> messagePayload) noexcept;

    static internal::MessageConnection& createConnection(
        std::shared_ptr<State> state, std::optional<Identity> remoteIdentity) noexcept;
};

}  // namespace ymq
}  // namespace scaler

#pragma once

#include <cstdint>
#include <expected>
#include <map>
#include <memory>
#include <optional>
#include <queue>
#include <string>
#include <vector>

#include "scaler/error/error.h"
#include "scaler/utility/move_only_function.h"
#include "scaler/uv_ymq/address.h"
#include "scaler/uv_ymq/internal/accept_server.h"
#include "scaler/uv_ymq/internal/event_loop_thread.h"
#include "scaler/uv_ymq/internal/message_connection.h"
#include "scaler/uv_ymq/io_context.h"
#include "scaler/uv_ymq/typedefs.h"
#include "scaler/ymq/message.h"

namespace scaler {
namespace uv_ymq {

// A socket that binds to a local address and accepts messages from multiple remote peers.
//
// Thread-safe: all operations are scheduled onto the socket's event loop thread.
class BinderSocket {
public:
    using BindCallback = scaler::utility::MoveOnlyFunction<void(std::expected<Address, scaler::ymq::Error>)>;

    using SendMessageCallback = scaler::utility::MoveOnlyFunction<void(std::expected<void, scaler::ymq::Error>)>;

    using RecvMessageCallback =
        scaler::utility::MoveOnlyFunction<void(std::expected<scaler::ymq::Message, scaler::ymq::Error>)>;

    BinderSocket(IOContext& context, Identity identity) noexcept;

    ~BinderSocket() noexcept;

    BinderSocket(const BinderSocket&)            = delete;
    BinderSocket& operator=(const BinderSocket&) = delete;

    BinderSocket(BinderSocket&&) noexcept            = default;
    BinderSocket& operator=(BinderSocket&&) noexcept = default;

    const Identity& identity() const noexcept;

    // Bind to a TCP or IPC address string (e.g. "tcp://127.0.0.1:9000", "ipc://my_socket").
    //
    // Multiple bind calls are allowed: the socket will accept connections on all bound addresses.
    void bindTo(std::string address, BindCallback onBindCallback) noexcept;

    // Send a message to a remote identity.
    void sendMessage(
        Identity remoteIdentity, scaler::ymq::Bytes messagePayload, SendMessageCallback onMessageSent) noexcept;

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
        scaler::ymq::Bytes messagePayload;
        SendMessageCallback onMessageSent;
    };

    struct State {
        internal::EventLoopThread& _thread;

        const Identity _identity;

        // Support binding to multiple addresses (TCP and/or IPC)
        std::vector<internal::AcceptServer> _servers {};

        ConnectionID _connectionCounter {0};

        std::map<ConnectionID, internal::MessageConnection> _connections {};
        std::map<Identity, ConnectionID> _identityToConnectionID {};

        std::map<Identity, std::vector<PendingSendMessage>> _pendingSendMessages {};

        std::queue<RecvMessageCallback> _pendingRecvCallbacks {};
        std::queue<scaler::ymq::Message> _pendingRecvMessages {};

        State(internal::EventLoopThread& thread, Identity identity) noexcept
            : _thread(thread), _identity(std::move(identity))
        {
        }
    };

    std::shared_ptr<State> _state;

    static void onClientConnect(std::shared_ptr<State> state, Client client) noexcept;

    static void onRemoteIdentity(
        std::shared_ptr<State> state, ConnectionID connectionId, Identity remoteIdentity) noexcept;

    static void onRemoteDisconnect(
        std::shared_ptr<State> state,
        ConnectionID connectionId,
        internal::MessageConnection::DisconnectReason reason) noexcept;

    static void onMessage(
        std::shared_ptr<State> state, ConnectionID connectionId, scaler::ymq::Bytes messagePayload) noexcept;

    static internal::MessageConnection& createConnection(
        std::shared_ptr<State> state, std::optional<Identity> remoteIdentity) noexcept;
};

}  // namespace uv_ymq
}  // namespace scaler

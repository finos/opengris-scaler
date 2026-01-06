#pragma once

#include "scaler/ymq/internal/socket_address.h"
#ifdef _WIN32
// clang-format off
#include <winsock2.h>
#include <mswsock.h>
// clang-format on
#undef SendMessageCallback
#endif  // _WIN32

// C++
#include <map>
#include <memory>
#include <optional>
#include <queue>
#include <string>

// First-party
#include "scaler/ymq/configuration.h"
#include "scaler/ymq/message.h"
#include "scaler/ymq/stream_client.h"
#include "scaler/ymq/stream_server.h"
#include "scaler/ymq/typedefs.h"

namespace scaler {
namespace ymq {

class EventLoopThread;
class MessageConnection;
class TcpWriteOperation;

class IOSocket {
public:
    using ConnectReturnCallback = Configuration::ConnectReturnCallback;
    using BindReturnCallback    = Configuration::BindReturnCallback;
    using SendMessageCallback   = Configuration::SendMessageCallback;
    using RecvMessageCallback   = Configuration::RecvMessageCallback;
    using Identity              = Configuration::IOSocketIdentity;

    IOSocket(std::shared_ptr<EventLoopThread> eventLoopThread, Identity identity, IOSocketType socketType) noexcept;
    IOSocket(const IOSocket&)            = delete;
    IOSocket& operator=(const IOSocket&) = delete;
    IOSocket(IOSocket&&)                 = delete;
    IOSocket& operator=(IOSocket&&)      = delete;
    ~IOSocket() noexcept;

    // NOTE: BELOW FOUR FUNCTIONS ARE USERSPACE API
    void sendMessage(Message message, SendMessageCallback onMessageSent) noexcept;
    void recvMessage(RecvMessageCallback onRecvMessage) noexcept;
    void bindTo(std::string netOrDomainAddr, BindReturnCallback onBindReturn) noexcept;
    void connectTo(
        std::string netOrDomainAddr, ConnectReturnCallback onConnectReturn, size_t maxRetryTimes = 4) noexcept;

    // NOTE: BELOW ONE ARE NOT OFFICIAL USERSPACE API. USE WITH CAUTION.
    void connectTo(SocketAddress addr, ConnectReturnCallback onConnectReturn, size_t maxRetryTimes = 4) noexcept;

    void closeConnection(Identity remoteSocketIdentity) noexcept;

    [[nodiscard]] constexpr Identity identity() const { return _identity; }

    [[nodiscard]] constexpr IOSocketType socketType() const { return _socketType; }

    // From Connection Class only
    // TODO: Maybe figure out a better name than keepInBook. When keepInBook is true, the system will remember this
    // remote identity and will treat the next connection with that identity as the reincarnation of this identity.
    // Thus, keeping the identity in the book.
    void onConnectionDisconnected(MessageConnection* conn, bool keepInBook = true) noexcept;
    // From Connection Class only
    void onConnectionIdentityReceived(MessageConnection* conn) noexcept;

    // From CONNECTOR only
    void onConnectorMaxedOutRetry() noexcept;

    // NOTE: These two functions are called respectively by sendMessage and server/client.
    // Notice that in the each case only the needed information are passed in; so it's less
    // likely the user passed in combinations that does not make sense. These two calls are
    // mutual exclusive. Perhaps we need better name, but I failed to come up with one. - gxu
    void onConnectionCreated(std::string remoteIOSocketIdentity) noexcept;
    void onConnectionCreated(
        int fd, SocketAddress localAddr, SocketAddress remoteAddr, bool responsibleForRetry) noexcept;

    // From StreamClient class only
    void removeConnectedStreamClient(const StreamClient* client) noexcept;

    void requestStop() noexcept;

    std::shared_ptr<EventLoopThread> _eventLoopThread;

    size_t numOfConnections();

private:
    void fillPendingRecvMessagesWithErr(Error err);

    const Identity _identity;
    const IOSocketType _socketType;

    // NOTE: Owning one TCPClient means the user cannot issue another connectTo
    // when some message connection is retring to connect.
    std::optional<StreamClient> _tcpClient;

    // NOTE: Owning one TCPServer means the user cannot bindTo multiple addresses.
    std::optional<StreamServer> _tcpServer;

    // NOTE: User may choose to bind to one IPv4 address + one IPC address
    std::optional<StreamServer> _ipcServer;
    std::optional<StreamClient> _ipcClient;

    // Remote identity to connection map
    std::map<std::string, std::unique_ptr<MessageConnection>> _identityToConnection;

    // NOTE: An unestablished connection can be in the following states:
    //  1. The underlying socket is not yet defined. This happens when user call sendMessage
    //  before connectTo finishes.
    //  2. The underlying connection haven't exchange remote identity with its peer. This
    //  happens upon new socket being created.
    //  3. The underlying connection contains peer's identity, but connection is broken. This
    //  happens upon remote end close the socket (or network issue).
    //  On the other hand, `Established Connection` are stored in _identityToConnection map.
    //  An established connection is a network connection that is currently connected, and
    //  exchanged their identity.
    std::vector<std::unique_ptr<MessageConnection>> _unestablishedConnection;

    // NOTE: This variable needs to present in the IOSocket level because the user
    // does not care which connection a message is coming from.
    std::queue<RecvMessageCallback> _pendingRecvMessages;

    std::queue<Message> _leftoverMessagesAfterConnectionDied;

    bool _stopped;
    bool _connectorDisconnected;
};

}  // namespace ymq
}  // namespace scaler

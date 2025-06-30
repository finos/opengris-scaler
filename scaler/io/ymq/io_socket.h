#pragma once

// C++
#include <map>
#include <memory>
#include <optional>
#include <queue>

// First-party
#include "scaler/io/ymq/configuration.h"
#include "scaler/io/ymq/message.h"
#include "scaler/io/ymq/tcp_client.h"
#include "scaler/io/ymq/tcp_server.h"
#include "scaler/io/ymq/typedefs.h"

class TcpReadOperation;
class MessageConnectionTCP;

// NOTE: Don't do this. It pollutes the env. I tried to remove it, but it reports err
// in pymod module. Consider include the corresponding file and define types there. - gxu
using Identity = Configuration::IOSocketIdentity;

class EventLoopThread;
class MessageConnectionTCP;

class IOSocket {
public:
    std::shared_ptr<EventLoopThread> _eventLoopThread;

private:
    const Identity _identity;
    const IOSocketType _socketType;

    // NOTE: Owning one TcpClient means the user cannot issue another connectTo
    // when some message connection is retring to connect.
    std::optional<TcpClient> _tcpClient;

    // NOTE: Owning one TcpServer means the user cannot bindTo multiple addresses.
    std::optional<TcpServer> _tcpServer;

    std::map<std::string, std::unique_ptr<MessageConnectionTCP>> _identityToConnection;

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
    std::vector<std::unique_ptr<MessageConnectionTCP>> _unestablishedConnection;

    // NOTE: This variable needs to present in the IOSocket level because the user
    // does not care which connection a message is coming from.
    std::shared_ptr<std::queue<TcpReadOperation>> _pendingReadOperations;

public:
    using ConnectReturnCallback = Configuration::ConnectReturnCallback;
    using BindReturnCallback    = Configuration::BindReturnCallback;
    using SendMessageCallback   = Configuration::SendMessageCallback;
    using RecvMessageCallback   = Configuration::RecvMessageCallback;

    IOSocket(std::shared_ptr<EventLoopThread> eventLoopThread, Identity identity, IOSocketType socketType);

    // NOTE: BELOW FIVE FUNCTIONS ARE USERSPACE API
    void sendMessage(Message message, SendMessageCallback callback);
    void recvMessage(RecvMessageCallback callback);

    void connectTo(sockaddr addr, ConnectReturnCallback callback);
    void connectTo(std::string networkAddress, ConnectReturnCallback callback);

    void bindTo(std::string networkAddress, BindReturnCallback callback);

    // From Connection Class only
    void onConnectionDisconnected(MessageConnectionTCP* conn);
    // From Connection Class only
    void onConnectionIdentityReceived(MessageConnectionTCP* conn);

    // This function is called whenever a connecition is created (not established)
    void onConnectionCreated(
        int fd,
        sockaddr localAddr,
        sockaddr remoteAddr,
        bool responsibleForRetry,
        std::optional<std::string> remoteIOSocketIdentity = std::nullopt);

    // From TcpClient class only
    void removeConnectedTcpClient();

    Identity identity() const { return _identity; }
    IOSocketType socketType() const { return _socketType; }

    IOSocket(const IOSocket&)            = delete;
    IOSocket& operator=(const IOSocket&) = delete;
    IOSocket(IOSocket&&)                 = delete;
    IOSocket& operator=(IOSocket&&)      = delete;
    ~IOSocket();
};

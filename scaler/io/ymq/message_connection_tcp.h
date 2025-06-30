#pragma once

#include <memory>
#include <optional>
#include <queue>
#include <vector>

#include "scaler/io/ymq/configuration.h"
#include "scaler/io/ymq/io_socket.h"
#include "scaler/io/ymq/message_connection.h"

class EventLoopThread;
class EventManager;

struct TcpWriteOperation {
    using SendMessageCallback = Configuration::SendMessageCallback;
    std::shared_ptr<std::vector<char>> _buf;
    size_t _cursor = 0;
    SendMessageCallback _callbackAfterCompleteWrite;
};

struct TcpReadOperation {
    using RecvMessageCallback = Configuration::RecvMessageCallback;
    std::shared_ptr<std::vector<char>> _buf;
    size_t _cursor = 0;
    RecvMessageCallback _callbackAfterCompleteRead;
};

class MessageConnectionTCP: public MessageConnection {
    int _connFd;
    sockaddr _localAddr;
    std::string _localIOSocketIdentity;
    bool _sendLocalIdentity;
    std::unique_ptr<EventManager> _eventManager;
    std::queue<TcpWriteOperation> _writeOperations;
    std::shared_ptr<std::queue<TcpReadOperation>> _pendingReadOperations;
    std::queue<std::vector<char>> _receivedMessages;

    void onRead();
    void onWrite();
    void onClose();
    void onError() {
        printf("onError (for debug don't remove)\n");
        exit(1);
    };

    std::shared_ptr<EventLoopThread> _eventLoopThread;

public:
    using SendMessageCallback = Configuration::SendMessageCallback;
    using RecvMessageCallback = Configuration::RecvMessageCallback;

    const sockaddr _remoteAddr;
    std::optional<std::string> _remoteIOSocketIdentity;
    const bool _responsibleForRetry;

    MessageConnectionTCP(
        std::shared_ptr<EventLoopThread> eventLoopThread,
        int connFd,
        sockaddr localAddr,
        sockaddr remoteAddr,
        std::string localIOSocketIdentity,
        bool responsibleForRetry,
        std::shared_ptr<std::queue<TcpReadOperation>> _pendingReadOperations,
        std::optional<std::string> remoteIOSocketIdentity = std::nullopt);

    void sendMessage(std::shared_ptr<std::vector<char>> msg, SendMessageCallback callback);
    bool recvMessage();

    void onCreated();

    ~MessageConnectionTCP();

    friend void IOSocket::onConnectionIdentityReceived(MessageConnectionTCP* conn);
};

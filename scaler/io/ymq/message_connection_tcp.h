#pragma once

#include <deque>
#include <memory>
#include <optional>
#include <queue>

#include "scaler/io/ymq/configuration.h"
#include "scaler/io/ymq/io_socket.h"
#include "scaler/io/ymq/message_connection.h"
#include "scaler/io/ymq/tcp_operations.h"

namespace scaler {
namespace ymq {

class EventLoopThread;
class EventManager;

class MessageConnectionTCP: public MessageConnection {
public:
    using SendMessageCallback = Configuration::SendMessageCallback;
    using RecvMessageCallback = Configuration::RecvMessageCallback;

    MessageConnectionTCP(
        std::shared_ptr<EventLoopThread> eventLoopThread,
        int connFd,
        sockaddr localAddr,
        sockaddr remoteAddr,
        std::string localIOSocketIdentity,
        bool responsibleForRetry,
        std::shared_ptr<std::queue<RecvMessageCallback>> _pendingRecvMessageCallbacks) noexcept;

    MessageConnectionTCP(
        std::shared_ptr<EventLoopThread> eventLoopThread,
        std::string localIOSocketIdentity,
        std::string remoteIOSocketIdentity,
        std::shared_ptr<std::queue<RecvMessageCallback>> _pendingRecvMessageCallbacks) noexcept;

    ~MessageConnectionTCP() noexcept;

    void onCreated();

    void sendMessage(Message msg, SendMessageCallback onMessageSent);
    bool recvMessage();

    std::shared_ptr<EventLoopThread> _eventLoopThread;
    const sockaddr _remoteAddr;
    const bool _responsibleForRetry;
    std::optional<std::string> _remoteIOSocketIdentity;

private:
    void onRead();
    void onWrite();
    void onClose();
    void onError()
    {
        printf("%s\n", __PRETTY_FUNCTION__);
        printf("onError (for debug don't remove) later this will be a log\n");
        onClose();
    };

    std::expected<void, int> tryReadMessages(bool readOneMessage);
    std::expected<size_t, int> trySendQueuedMessages();
    void updateWriteOperations(size_t n);
    void updateReadOperation();

    std::unique_ptr<EventManager> _eventManager;
    int _connFd;
    sockaddr _localAddr;
    std::string _localIOSocketIdentity;

    std::deque<TcpWriteOperation> _writeOperations;
    size_t _sendCursor;

    std::shared_ptr<std::queue<RecvMessageCallback>> _pendingRecvMessageCallbacks;
    std::queue<TcpReadOperation> _receivedReadOperations;

    constexpr static bool isCompleteMessage(const TcpReadOperation& x);
    friend void IOSocket::onConnectionIdentityReceived(MessageConnectionTCP* conn) noexcept;
    friend void IOSocket::onConnectionDisconnected(MessageConnectionTCP* conn) noexcept;
};

}  // namespace ymq
}  // namespace scaler

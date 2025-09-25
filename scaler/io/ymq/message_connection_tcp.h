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
    void disconnect();

    std::shared_ptr<EventLoopThread> _eventLoopThread;
    const sockaddr _remoteAddr;
    const bool _responsibleForRetry;
    std::optional<std::string> _remoteIOSocketIdentity;

private:
    enum class IOError {
        Drained,
        Aborted,
        Disconnected,
        MessageTooLarge,
    };

    void onRead();
    void onWrite();
    void onClose();
    void onError()
    {
        onRead();
        // onClose();
    };

    std::expected<void, IOError> tryReadOneMessage();
    std::expected<void, IOError> tryReadMessages();
    std::expected<size_t, IOError> trySendQueuedMessages();
    void updateWriteOperations(size_t n);
    void updateReadOperation();

    void setRemoteIdentity() noexcept;

    std::unique_ptr<EventManager> _eventManager;
    int _connFd;
    sockaddr _localAddr;
    std::string _localIOSocketIdentity;

    std::deque<TcpWriteOperation> _writeOperations;
    size_t _sendCursor;

    std::shared_ptr<std::queue<RecvMessageCallback>> _pendingRecvMessageCallbacks;
    std::queue<TcpReadOperation> _receivedReadOperations;

    bool _disconnect;

    constexpr static bool isCompleteMessage(const TcpReadOperation& x);
    friend void IOSocket::onConnectionIdentityReceived(MessageConnectionTCP* conn) noexcept;
    friend void IOSocket::onConnectionDisconnected(MessageConnectionTCP* conn, bool keepInBook) noexcept;
};

}  // namespace ymq
}  // namespace scaler

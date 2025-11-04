#pragma once

#include <deque>
#include <memory>
#include <optional>
#include <queue>

#include "scaler/ymq/configuration.h"
#include "scaler/ymq/internal/raw_connection_tcp_fd.h"
#include "scaler/ymq/io_socket.h"
#include "scaler/logging/logging.h"
#include "scaler/ymq/message_connection.h"
#include "scaler/ymq/tcp_operations.h"

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
        std::queue<RecvMessageCallback>* _pendingRecvMessageCallbacks,
        std::queue<Message>* leftoverMessagesAfterConnectionDied) noexcept;

    MessageConnectionTCP(
        std::shared_ptr<EventLoopThread> eventLoopThread,
        std::string localIOSocketIdentity,
        std::string remoteIOSocketIdentity,
        std::queue<RecvMessageCallback>* _pendingRecvMessageCallbacks,
        std::queue<Message>* leftoverMessagesAfterConnectionDied) noexcept;

    ~MessageConnectionTCP() noexcept;

    void onCreated();

    void sendMessage(Message msg, SendMessageCallback onMessageSent);
    bool recvMessage();
    void disconnect();

    std::shared_ptr<EventLoopThread> _eventLoopThread;
    const sockaddr _remoteAddr;
    const bool _responsibleForRetry;
    std::optional<std::string> _remoteIOSocketIdentity;

    // Returns true when nativeHandle is _closed_, not shutdown.
    bool disconnected();

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
        if (_rawConn.nativeHandle()) {
            onRead();
        }
    };

    std::expected<void, IOError> tryReadOneMessage();
    std::expected<void, IOError> tryReadMessages();
    std::expected<size_t, IOError> trySendQueuedMessages();
    void updateWriteOperations(size_t n);
    void updateReadOperation();

    void setRemoteIdentity() noexcept;

    std::unique_ptr<EventManager> _eventManager;
    RawConnectionTCPFD _rawConn;
    sockaddr _localAddr;
    std::string _localIOSocketIdentity;

    std::deque<TcpWriteOperation> _writeOperations;
    size_t _sendCursor;

    std::queue<RecvMessageCallback>* _pendingRecvMessageCallbacks;
    std::queue<Message>* _leftoverMessagesAfterConnectionDied;

    std::queue<TcpReadOperation> _receivedReadOperations;

    bool _disconnect;  // Disconnect or Abort, use to feed to IOSocket
    Logger _logger;

    // TODO: This variable records whether we have read some bytes in the last read operation.
    // The semantic of readMessage is completely broken. But that will be fixed in the refactor.
    bool _readSomeBytes;

    constexpr static bool isCompleteMessage(const TcpReadOperation& x);
    friend void IOSocket::onConnectionIdentityReceived(MessageConnectionTCP* conn) noexcept;
    friend void IOSocket::onConnectionDisconnected(MessageConnectionTCP* conn, bool keepInBook) noexcept;
};

}  // namespace ymq
}  // namespace scaler

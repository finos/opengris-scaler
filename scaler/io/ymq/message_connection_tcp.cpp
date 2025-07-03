
#include "scaler/io/ymq/message_connection_tcp.h"

#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <optional>

#include "scaler/io/ymq/event_loop_thread.h"
#include "scaler/io/ymq/event_manager.h"
#include "scaler/io/ymq/io_socket.h"

static constexpr const size_t HEADER_SIZE = sizeof(uint64_t);

constexpr bool MessageConnectionTCP::isCompleteMessage(const TcpReadOperation& x) {
    if (x._cursor < HEADER_SIZE) {
        return false;
    }
    if (x._cursor == x._header + HEADER_SIZE && x._payload.data() != nullptr) {
        return true;
    }
    return false;
}

MessageConnectionTCP::MessageConnectionTCP(
    std::shared_ptr<EventLoopThread> eventLoopThread,
    int connFd,
    sockaddr localAddr,
    sockaddr remoteAddr,
    std::string localIOSocketIdentity,
    bool responsibleForRetry,
    std::shared_ptr<std::queue<RecvMessageCallback>> pendingRecvMessageCallbacks,
    std::optional<std::string> remoteIOSocketIdentity) noexcept
    : _eventLoopThread(eventLoopThread)
    , _eventManager(std::make_unique<EventManager>())
    , _connFd(std::move(connFd))
    , _localAddr(std::move(localAddr))
    , _remoteAddr(std::move(remoteAddr))
    , _localIOSocketIdentity(std::move(localIOSocketIdentity))
    , _remoteIOSocketIdentity(std::move(remoteIOSocketIdentity))
    , _responsibleForRetry(responsibleForRetry)
    , _pendingRecvMessageCallbacks(pendingRecvMessageCallbacks)
    , _sendCursor {} {
    _eventManager->onRead  = [this] { this->onRead(); };
    _eventManager->onWrite = [this] { this->onWrite(); };
    _eventManager->onClose = [this] { this->onClose(); };
    _eventManager->onError = [this] { this->onError(); };
}

void MessageConnectionTCP::onCreated() {
    if (_connFd != 0) {
        this->_eventLoopThread->_eventLoop.addFdToLoop(
            _connFd, EPOLLIN | EPOLLOUT | EPOLLET, this->_eventManager.get());
        _writeOperations.emplace_back(Bytes {_localIOSocketIdentity.data(), _localIOSocketIdentity.size()}, [](int) {});
    }
}

// on Return, unexpected value shall be interpreted as this - 0 = close, other -> errno
std::expected<void, int> MessageConnectionTCP::tryReadMessages() {
    while (true) {
        char* readTo         = nullptr;
        size_t remainingSize = 0;

        if (_receivedReadOperations.empty() || isCompleteMessage(_receivedReadOperations.front())) {
            _receivedReadOperations.emplace();
        }

        auto& message = _receivedReadOperations.back();
        if (message._cursor < HEADER_SIZE) {
            readTo        = (char*)&message._header + message._cursor;
            remainingSize = HEADER_SIZE - message._cursor;
        } else if (message._cursor == HEADER_SIZE) {
            message._payload = Bytes::alloc(message._header);
            readTo           = (char*)message._payload.data();
            remainingSize    = message._payload.len();
        } else {
            readTo        = (char*)message._payload.data() + (message._cursor - HEADER_SIZE);
            remainingSize = message._payload.len() - (message._cursor - HEADER_SIZE);
        }

        // We have received an empty message, which is allowed
        if (remainingSize == 0) {
            return {};
        }

        int n = read(_connFd, readTo, remainingSize);
        if (n == 0) {
            return std::unexpected {0};
        } else if (n == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return {};  // Expected, we read until exhuastion
            } else {
                return std::unexpected {errno};
            }
        } else {
            message._cursor += n;
        }
    }
    return {};
}

void MessageConnectionTCP::updateReadOperation() {
    while (_pendingRecvMessageCallbacks->size() && _receivedReadOperations.size()) {
        if (isCompleteMessage(_receivedReadOperations.front())) {
            Bytes address(_remoteIOSocketIdentity->data(), _remoteIOSocketIdentity->size());
            Bytes payload(std::move(_receivedReadOperations.front()._payload));
            _receivedReadOperations.pop();

            auto recvMessageCallback = std::move(_pendingRecvMessageCallbacks->front());
            _pendingRecvMessageCallbacks->pop();

            recvMessageCallback(Message(std::move(address), std::move(payload)));

        } else {
            assert(_pendingRecvMessageCallbacks->size());
            break;
        }
    }
}

void MessageConnectionTCP::onRead() {
    auto res = tryReadMessages();
    if (!res) {
        if (res.error() == 0) {
            onClose();
            return;
        }
        printf("SOMETHING REALLY BAD HAPPENED\n");
        exit(1);
    }

    if (!_remoteIOSocketIdentity) {
        if (_receivedReadOperations.size() && isCompleteMessage(_receivedReadOperations.front())) {
            auto id = std::move(_receivedReadOperations.front());
            _remoteIOSocketIdentity.emplace((char*)id._payload.data(), id._payload.len());
            _receivedReadOperations.pop();
            auto sock = this->_eventLoopThread->_identityToIOSocket[_localIOSocketIdentity];
            sock->onConnectionIdentityReceived(this);
        }
    }

    updateReadOperation();
}

void MessageConnectionTCP::onWrite() {
    // This is because after disconnected, onRead will be called first, and that will set
    // _connFd to 0. There's no way to not call onWrite in this case. So we return early.
    if (_connFd == 0) {
        return;
    }

    auto res = trySendQueuedMessages();
    if (res) {
        updateWriteOperations(res.value());
        return;
    }

    // EPIPE: Shutdown for writing or disconnected, since we don't provide the former,
    // it means the later.
    if (res.error() == ECONNRESET || res.error() == EPIPE) {
        onClose();
        return;
    } else {
        printf("SOMETHING REALLY BAD\n");
        exit(1);
    }
}

void MessageConnectionTCP::onClose() {
    if (_connFd) {
        _eventLoopThread->_eventLoop.removeFdFromLoop(_connFd);
        close(_connFd);
        _connFd    = 0;
        auto& sock = _eventLoopThread->_identityToIOSocket.at(_localIOSocketIdentity);
        sock->onConnectionDisconnected(this);
    }
};

std::expected<size_t, int> MessageConnectionTCP::trySendQueuedMessages() {
    std::vector<struct iovec> iovecs;
    iovecs.reserve(IOV_MAX);

    for (auto it = _writeOperations.begin(); it != _writeOperations.end(); ++it) {
        if (iovecs.size() > IOV_MAX - 2) {
            break;
        }

        iovec iovHeader {};
        iovec iovPayload {};
        if (it == _writeOperations.begin()) {
            if (_sendCursor < HEADER_SIZE) {
                iovHeader.iov_base  = (char*)(&it->_header) + _sendCursor;
                iovHeader.iov_len   = HEADER_SIZE - _sendCursor;
                iovPayload.iov_base = (void*)(it->_payload.data());
                iovPayload.iov_len  = it->_payload.len();
            } else {
                iovHeader.iov_base  = nullptr;
                iovHeader.iov_len   = 0;
                iovPayload.iov_base = (char*)(it->_payload.data()) + (_sendCursor - HEADER_SIZE);
                iovPayload.iov_len  = it->_payload.len() + (_sendCursor - HEADER_SIZE);
            }
        } else {
            iovHeader.iov_base  = (void*)(&it->_header);
            iovHeader.iov_len   = HEADER_SIZE;
            iovPayload.iov_base = (void*)(it->_payload.data());
            iovPayload.iov_len  = it->_payload.len();
        }

        iovecs.push_back(iovHeader);
        iovecs.push_back(iovPayload);
    }

    if (iovecs.empty()) {
        return 0;
    }

    struct msghdr msg {};
    msg.msg_iov    = iovecs.data();
    msg.msg_iovlen = iovecs.size();

    ssize_t bytesSent = ::sendmsg(_connFd, &msg, MSG_NOSIGNAL);
    if (bytesSent == -1) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return 0;
        } else
            return std::unexpected {errno};
    }

    return bytesSent;
}

// TODO: There is a classic optimization that can (and should) be done. That is, we store
// prefix sum in each write operation, and perform binary search instead of linear search
// to find the first write operation we haven't complete. - gxu
void MessageConnectionTCP::updateWriteOperations(size_t n) {
    auto firstIncomplete = _writeOperations.begin();
    _sendCursor += n;
    // Post condition of the loop: firstIncomplete contains the first write op we haven't complete.
    for (auto it = _writeOperations.begin(); it != _writeOperations.end(); ++it) {
        size_t msgSize = it->_payload.len() + HEADER_SIZE;
        if (_sendCursor < msgSize) {
            firstIncomplete = it;
            break;
        }

        if (_sendCursor == msgSize) {
            firstIncomplete = it + 1;
            _sendCursor     = 0;
            break;
        }

        _sendCursor -= msgSize;
    }

    for (auto it = _writeOperations.begin(); it != firstIncomplete; ++it) {
        it->_callbackAfterCompleteWrite(0);
    }

    while (firstIncomplete != _writeOperations.begin())
        _writeOperations.pop_front();

    // _writeOperations.shrink_to_fit();
}

void MessageConnectionTCP::sendMessage(Message msg, SendMessageCallback onMessageSent) {
    TcpWriteOperation writeOp(std::move(msg), std::move(onMessageSent));
    _writeOperations.emplace_back(std::move(writeOp));

    if (_connFd == 0) {
        return;
    }
    onWrite();
}

bool MessageConnectionTCP::recvMessage() {
    if (_receivedReadOperations.empty() || _pendingRecvMessageCallbacks->empty() ||
        !isCompleteMessage(_receivedReadOperations.front())) {
        return false;
    }

    updateReadOperation();
    return true;
}

MessageConnectionTCP::~MessageConnectionTCP() noexcept {
    if (_connFd != 0) {
        _eventLoopThread->_eventLoop.removeFdFromLoop(_connFd);
        shutdown(_connFd, SHUT_RDWR);
        close(_connFd);
        _connFd = 0;
    }

    std::ranges::for_each(_writeOperations, [](auto&& x) { x._callbackAfterCompleteWrite(-1); });

    // TODO: What to do with this?
    // std::queue<std::vector<char>> _receivedMessages;
}

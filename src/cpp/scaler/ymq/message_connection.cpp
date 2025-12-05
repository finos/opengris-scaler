
#include "scaler/ymq/message_connection.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <expected>
#include <functional>
#include <memory>
#include <new>
#include <optional>
#include <utility>

#include "scaler/error/error.h"
#include "scaler/ymq/configuration.h"
#include "scaler/ymq/event_loop_thread.h"
#include "scaler/ymq/event_manager.h"
#include "scaler/ymq/internal/socket_address.h"
#include "scaler/ymq/io_socket.h"

namespace scaler {
namespace ymq {

static constexpr const size_t HEADER_SIZE = sizeof(uint64_t);

constexpr bool MessageConnection::isCompleteMessage(const TcpReadOperation& x)
{
    if (x._cursor < HEADER_SIZE) {
        return false;
    }
    if (x._cursor == x._header + HEADER_SIZE && x._payload.data() != nullptr) {
        return true;
    }
    return false;
}

MessageConnection::MessageConnection(
    EventLoopThread* eventLoopThread,
    int connFd,
    SocketAddress localAddr,
    SocketAddress remoteAddr,
    std::string localIOSocketIdentity,
    bool responsibleForRetry,
    std::queue<RecvMessageCallback>* pendingRecvMessageCallbacks,
    std::queue<Message>* leftoverMessagesAfterConnectionDied) noexcept
    : _eventLoopThread(eventLoopThread)
    , _remoteAddr(std::move(remoteAddr))
    , _responsibleForRetry(responsibleForRetry)
    , _remoteIOSocketIdentity(std::nullopt)
    , _eventManager(std::make_unique<EventManager>())
    , _rawConn(std::move(connFd))
    , _localAddr(std::move(localAddr))
    , _localIOSocketIdentity(std::move(localIOSocketIdentity))
    , _sendCursor {}
    , _pendingRecvMessageCallbacks(pendingRecvMessageCallbacks)
    , _leftoverMessagesAfterConnectionDied(leftoverMessagesAfterConnectionDied)
    , _disconnect {false}
{
    _eventManager->onRead  = [this] { this->onRead(); };
    _eventManager->onWrite = [this] { this->onWrite(); };
    _eventManager->onClose = [this] { this->onClose(); };
    _eventManager->onError = [this] { this->onError(); };
}

MessageConnection::MessageConnection(
    EventLoopThread* eventLoopThread,
    std::string localIOSocketIdentity,
    std::string remoteIOSocketIdentity,
    std::queue<RecvMessageCallback>* pendingRecvMessageCallbacks,
    std::queue<Message>* leftoverMessagesAfterConnectionDied) noexcept
    : _eventLoopThread(eventLoopThread)
    , _remoteAddr {}
    , _responsibleForRetry(false)
    , _remoteIOSocketIdentity(std::move(remoteIOSocketIdentity))
    , _eventManager(std::make_unique<EventManager>())
    , _rawConn {}
    , _localAddr {}
    , _localIOSocketIdentity(std::move(localIOSocketIdentity))
    , _sendCursor {}
    , _pendingRecvMessageCallbacks(pendingRecvMessageCallbacks)
    , _leftoverMessagesAfterConnectionDied(leftoverMessagesAfterConnectionDied)
    , _disconnect {false}
    , _readSomeBytes {false}
{
    _eventManager->onRead  = [this] { this->onRead(); };
    _eventManager->onWrite = [this] { this->onWrite(); };
    _eventManager->onClose = [this] { this->onClose(); };
    _eventManager->onError = [this] { this->onError(); };
}

void MessageConnection::onCreated()
{
    if (_rawConn.nativeHandle() != 0) {
        this->_eventLoopThread->_eventLoop.addFdToLoop(
            _rawConn.nativeHandle(), EPOLLIN | EPOLLOUT | EPOLLET, this->_eventManager.get());
        _writeOperations.emplace_back(
            Bytes {_localIOSocketIdentity.data(), _localIOSocketIdentity.size()}, [](auto) {});

        const size_t len = 1;
        const auto [n, immediateResult] =
            _rawConn.prepareWriteBytes(&_writeOperations.back()._header, len, _eventManager.get());
        (void)immediateResult;

        updateWriteOperations(n);

        _rawConn.prepareReadBytes(this->_eventManager.get());
    }
}

bool MessageConnection::disconnected()
{
    return _rawConn.nativeHandle() == 0;
}

std::expected<void, MessageConnection::IOError> MessageConnection::tryReadOneMessage()
{
    if (_receivedReadOperations.empty() || isCompleteMessage(_receivedReadOperations.back())) {
        _receivedReadOperations.emplace();
    }
    while (!isCompleteMessage(_receivedReadOperations.back())) {
        char* readTo         = nullptr;
        size_t remainingSize = 0;

        auto& message = _receivedReadOperations.back();
        if (message._cursor < HEADER_SIZE) {
            readTo        = (char*)&message._header + message._cursor;
            remainingSize = HEADER_SIZE - message._cursor;
        } else if (message._cursor == HEADER_SIZE) {
            // NOTE: We probably need a better protocol to solve this issue completely, but this should let us pin down
            // why OSS sometimes throws bad_alloc
            try {
                // On Linux, this will never happen because this function is only called when
                // new read comes in. On other platform, this might be different.
                if (!message._payload.data()) {
                    message._payload = Bytes::alloc(message._header);
                }
                readTo        = (char*)message._payload.data();
                remainingSize = message._payload.len();
            } catch (const std::bad_alloc& e) {
                _logger.log(
                    Logger::LoggingLevel::error,
                    "Trying to allocate ",
                    message._header,
                    " bytes.",
                    " bad_alloc caught, connection closed");
                return std::unexpected {IOError::MessageTooLarge};
            }
        } else {
            readTo        = (char*)message._payload.data() + (message._cursor - HEADER_SIZE);
            remainingSize = message._payload.len() - (message._cursor - HEADER_SIZE);
        }

        // We have received an empty message, which is allowed
        if (remainingSize == 0) {
            return {};
        }

        auto [bytesRead, status] = _rawConn.tryReadUntilComplete(readTo, remainingSize);
        message._cursor += bytesRead;

        if (bytesRead > 0) {
            _readSomeBytes = true;
        }

        if (status != RawStreamConnectionHandle::IOStatus::MoreBytesAvailable) {
            switch (status) {
                case RawStreamConnectionHandle::IOStatus::Aborted: {
                    return std::unexpected {IOError::Aborted};
                }
                case RawStreamConnectionHandle::IOStatus::Disconnected: {
                    return std::unexpected {IOError::Disconnected};
                }
                case RawStreamConnectionHandle::IOStatus::Drained: {
                    return std::unexpected {IOError::Drained};
                }
                case RawStreamConnectionHandle::IOStatus::MoreBytesAvailable: {
                    std::unreachable();
                }
            }
        }
    }
    return {};
}

// on Return, unexpected value shall be interpreted as this - 0 = close, other -> errno
std::expected<void, MessageConnection::IOError> MessageConnection::tryReadMessages()
{
    while (true) {
        auto res = tryReadOneMessage();
        if (!res) {
            return res;
        }
    }
}

void MessageConnection::updateReadOperation()
{
    while (_pendingRecvMessageCallbacks->size() && _receivedReadOperations.size()) {
        if (isCompleteMessage(_receivedReadOperations.front())) {
            Bytes address(_remoteIOSocketIdentity->data(), _remoteIOSocketIdentity->size());
            Bytes payload(std::move(_receivedReadOperations.front()._payload));
            _receivedReadOperations.pop();
            auto recvMessageCallback = std::move(_pendingRecvMessageCallbacks->front());
            _pendingRecvMessageCallbacks->pop();

            recvMessageCallback({Message(std::move(address), std::move(payload)), {}});
        } else {
            assert(_pendingRecvMessageCallbacks->size());
            break;
        }
    }
}

void MessageConnection::setRemoteIdentity() noexcept
{
    if (!_remoteIOSocketIdentity &&
        (_receivedReadOperations.size() && isCompleteMessage(_receivedReadOperations.front()))) {
        auto id = std::move(_receivedReadOperations.front());
        _remoteIOSocketIdentity.emplace((char*)id._payload.data(), id._payload.len());
        _receivedReadOperations.pop();
        auto sock = this->_eventLoopThread->_identityToIOSocket[_localIOSocketIdentity];
        sock->onConnectionIdentityReceived(this);
    }
}

void MessageConnection::onRead()
{
    if (_rawConn.nativeHandle() == 0) {
        return;
    }

    _readSomeBytes = false;

    if (!_remoteIOSocketIdentity) {
        auto maybeIdentity = tryReadOneMessage();
        if (maybeIdentity) {
            setRemoteIdentity();
        } else {
            switch (maybeIdentity.error()) {
                case IOError::Aborted:
                    _disconnect = false;
                    onClose();
                    return;
                case IOError::Disconnected:
                    _disconnect = true;
                    onClose();
                    return;
                case IOError::MessageTooLarge:
                    _disconnect = true;
                    onClose();
                    return;

                case IOError::Drained: {
                    _rawConn.prepareReadBytes(this->_eventManager.get());
                    return;
                }
            }
        }
    }

    auto maybeMessages = tryReadMessages();
    updateReadOperation();
    if (!maybeMessages) {
        switch (maybeMessages.error()) {
            case IOError::Aborted:
                _disconnect = false;
                onClose();
                return;
            case IOError::Disconnected:
                _disconnect = true;
                onClose();
                return;
            case IOError::MessageTooLarge:
                _disconnect = true;
                onClose();
                return;
            case IOError::Drained: break;
        }
    }

    // NOTE:
    // Because there was no way to differentiate Read/Write operation on Windows, current behaviour is that whenever an
    // operation arrives, I call both onRead and onWrite. This creates erroneous behaviour as we are posting too much
    // operations into the queue maintained by the kernel.
    // Sometimes, we don't really need to queued in another operation, as we typically know that when onRead is being
    // called with no bytes being read, we know this is a false positive call (introduce by a write-available
    // notification for example) and the previous ReadFile notification is still in the operating system's kernel.
    // Perhaps, we should refactor tryReadOneMessage etc so that it returns bytes read.
    // We will do it in near future.
    if (!_readSomeBytes) {
        return;
    }

    _rawConn.prepareReadBytes(this->_eventManager.get());
}

void MessageConnection::onWrite()
{
    // This is because after disconnected, onRead will be called first, and that will set
    // _connFd to 0. There's no way to not call onWrite in this case. So we return early.
    if (_rawConn.nativeHandle() == 0) {
        return;
    }

    auto res = trySendQueuedMessages();
    if (res) {
        updateWriteOperations(res.value());
        return;
    }

    switch (res.error()) {
        case IOError::Aborted:
            _disconnect = false;
            onClose();
            return;
        case IOError::Disconnected:
            _disconnect = true;
            onClose();
            return;
        case IOError::MessageTooLarge: std::unreachable(); return;
        case IOError::Drained: break;
    }

    // NOTE: Precondition is the queue still has messages (perhaps a partial one).
    // We don't need to update the queue because trySendQueuedMessages is okay with a complete message in front.
    if (res.error() == IOError::Drained) {
        void* addr = nullptr;
        if (_sendCursor < HEADER_SIZE) {
            addr = (char*)(&_writeOperations.front()._header) + _sendCursor;
        } else {
            addr = (char*)_writeOperations.front()._payload.data() + _sendCursor - HEADER_SIZE;
        }

        const size_t len                = 1;
        const auto [n, immediateResult] = _rawConn.prepareWriteBytes(addr, len, _eventManager.get());
        // NOTE: We do need to update the queue afterwards, though
        updateWriteOperations(n);
        (void)immediateResult;
    }
}

void MessageConnection::onClose()
{
    if (_rawConn.nativeHandle()) {
        if (_remoteIOSocketIdentity) {
            while (_receivedReadOperations.size() && isCompleteMessage(_receivedReadOperations.front())) {
                Bytes address(_remoteIOSocketIdentity->data(), _remoteIOSocketIdentity->size());
                auto msg = std::move(_receivedReadOperations.front());
                Bytes payload(std::move(msg._payload));
                _leftoverMessagesAfterConnectionDied->emplace(std::move(address), std::move(payload));
                _receivedReadOperations.pop();
            }
        }

        _eventLoopThread->_eventLoop.removeFdFromLoop(_rawConn.nativeHandle());
        _rawConn.shutdownBoth();
        _rawConn.closeAndZero();
        auto& sock = _eventLoopThread->_identityToIOSocket.at(_localIOSocketIdentity);
        sock->onConnectionDisconnected(this, !_disconnect);
    }
};

std::expected<size_t, MessageConnection::IOError> MessageConnection::trySendQueuedMessages()
{
    // TODO: Should this accept 0 length send?
    if (_writeOperations.empty()) {
        return 0;
    }
    std::vector<std::pair<void*, size_t>> args;
    args.reserve(_writeOperations.size());
    for (auto it = _writeOperations.begin(); it != _writeOperations.end(); ++it) {
        std::pair<void*, size_t> header;
        std::pair<void*, size_t> payload;
        if (it == _writeOperations.begin()) {
            if (_sendCursor < HEADER_SIZE) {
                header.first   = (char*)(&it->_header) + _sendCursor;
                header.second  = HEADER_SIZE - _sendCursor;
                payload.first  = (char*)(it->_payload.data());
                payload.second = it->_payload.len();
            } else {
                header.first   = nullptr;
                header.second  = 0;
                payload.first  = (char*)(it->_payload.data()) + (_sendCursor - HEADER_SIZE);
                payload.second = it->_payload.len() - (_sendCursor - HEADER_SIZE);
            }
        } else {
            header.first   = (char*)(&it->_header);
            header.second  = HEADER_SIZE;
            payload.first  = (char*)(it->_payload.data());
            payload.second = it->_payload.len();
        }

        args.push_back(header);
        args.push_back(payload);
    }

    auto [n, status] = _rawConn.tryWriteUntilComplete(args);
    if (n > 0) {
        return n;
    }
    switch (status) {
        case RawStreamConnectionHandle::IOStatus::Drained: {
            return std::unexpected {IOError::Drained};
        }
        case RawStreamConnectionHandle::IOStatus::Disconnected: {
            return std::unexpected {IOError::Disconnected};
        }
        case RawStreamConnectionHandle::IOStatus::Aborted: {
            return std::unexpected {IOError::Aborted};
        }
        case RawStreamConnectionHandle::IOStatus::MoreBytesAvailable: {
            std::unreachable();
        }
    }
    std::unreachable();
}

// TODO: There is a classic optimization that can (and should) be done. That is, we store
// prefix sum in each write operation, and perform binary search instead of linear search
// to find the first write operation we haven't complete. - gxu
void MessageConnection::updateWriteOperations(size_t n)
{
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
        it->_callbackAfterCompleteWrite({});
    }

    const int numPopItems = std::distance(_writeOperations.begin(), firstIncomplete);
    for (int i = 0; i < numPopItems; ++i) {
        _writeOperations.pop_front();
    }

    // _writeOperations.shrink_to_fit();
}

void MessageConnection::sendMessage(Message msg, SendMessageCallback onMessageSent)
{
    TcpWriteOperation writeOp(std::move(msg), std::move(onMessageSent));
    _writeOperations.emplace_back(std::move(writeOp));

    if (_rawConn.nativeHandle() == 0) {
        return;
    }
    onWrite();
}

bool MessageConnection::recvMessage()
{
    if (_receivedReadOperations.empty() || _pendingRecvMessageCallbacks->empty() ||
        !isCompleteMessage(_receivedReadOperations.front())) {
        return false;
    }

    updateReadOperation();
    return true;
}

void MessageConnection::disconnect()
{
    if (_rawConn.nativeHandle()) {
        _rawConn.shutdownWrite();
    }
}

MessageConnection::~MessageConnection() noexcept
{
    if (_rawConn.nativeHandle() != 0) {
        _eventLoopThread->_eventLoop.removeFdFromLoop(_rawConn.nativeHandle());
    }

    std::ranges::for_each(_writeOperations, [](auto&& x) {
        x._callbackAfterCompleteWrite(std::unexpected {Error::ErrorCode::SendMessageRequestCouldNotComplete});
    });

    // TODO: What to do with this?
    // std::queue<std::vector<char>> _receivedMessages;
}

}  // namespace ymq
}  // namespace scaler

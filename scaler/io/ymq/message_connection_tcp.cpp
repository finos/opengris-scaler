
#include "scaler/io/ymq/message_connection_tcp.h"

#include <new>
#include <utility>

#include "scaler/io/ymq/configuration.h"
#include "scaler/io/ymq/system/raw_tcp_connection_fd.h"

#ifdef __linux__
#include <unistd.h>
#endif         // __linux__
#ifdef _WIN32  // TODO: Maybe this can be removed.
// clang-format off
#include <windows.h>
#include <winsock2.h>
#include <mswsock.h>
// clang-format on
#endif  // _WIN32

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <expected>
#include <functional>
#include <memory>
#include <optional>

#include "scaler/io/ymq/error.h"
#include "scaler/io/ymq/event_loop_thread.h"
#include "scaler/io/ymq/event_manager.h"
#include "scaler/io/ymq/io_socket.h"
#include "scaler/io/ymq/network_utils.h"

namespace scaler {
namespace ymq {

static constexpr const size_t HEADER_SIZE = sizeof(uint64_t);

constexpr bool MessageConnectionTCP::isCompleteMessage(const TcpReadOperation& x)
{
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
    std::shared_ptr<std::queue<RecvMessageCallback>> pendingRecvMessageCallbacks) noexcept
    : _eventLoopThread(eventLoopThread)
    , _remoteAddr(std::move(remoteAddr))
    , _responsibleForRetry(responsibleForRetry)
    , _remoteIOSocketIdentity(std::nullopt)
    , _eventManager(std::make_unique<EventManager>())
    , _connFd(std::move(connFd))
    , _localAddr(std::move(localAddr))
    , _localIOSocketIdentity(std::move(localIOSocketIdentity))
    , _sendCursor {}
    , _pendingRecvMessageCallbacks(pendingRecvMessageCallbacks)
    , _disconnect {false}
    , _rawTCPConnectionFD(_connFd)

{
    _eventManager->onRead  = [this] { this->onRead(); };
    _eventManager->onWrite = [this] { this->onWrite(); };
    _eventManager->onClose = [this] { this->onClose(); };
    _eventManager->onError = [this] { this->onError(); };
}

MessageConnectionTCP::MessageConnectionTCP(
    std::shared_ptr<EventLoopThread> eventLoopThread,
    std::string localIOSocketIdentity,
    std::string remoteIOSocketIdentity,
    std::shared_ptr<std::queue<RecvMessageCallback>> pendingRecvMessageCallbacks) noexcept
    : _eventLoopThread(eventLoopThread)
    , _remoteAddr {}
    , _responsibleForRetry(false)
    , _remoteIOSocketIdentity(std::move(remoteIOSocketIdentity))
    , _eventManager(std::make_unique<EventManager>())
    , _connFd {}
    , _localAddr {}
    , _localIOSocketIdentity(std::move(localIOSocketIdentity))
    , _sendCursor {}
    , _pendingRecvMessageCallbacks(pendingRecvMessageCallbacks)
    , _disconnect {false}
    , _rawTCPConnectionFD(_connFd)
{
    _eventManager->onRead  = [this] { this->onRead(); };
    _eventManager->onWrite = [this] { this->onWrite(); };
    _eventManager->onClose = [this] { this->onClose(); };
    _eventManager->onError = [this] { this->onError(); };
}

void MessageConnectionTCP::onCreated()
{
    if (_connFd != 0) {
#ifdef __linux__
        this->_eventLoopThread->_eventLoop.addFdToLoop(
            _connFd, EPOLLIN | EPOLLOUT | EPOLLET, this->_eventManager.get());
        _writeOperations.emplace_back(
            Bytes {_localIOSocketIdentity.data(), _localIOSocketIdentity.size()}, [](auto) {});
#endif  // __linux__
#ifdef _WIN32
        // TODO: Leave it as is, but remove it in the future (once we define EPOLLIN etc.)
        this->_eventLoopThread->_eventLoop.addFdToLoop(_connFd, 0, nullptr);
        _writeOperations.emplace_back(
            Bytes {_localIOSocketIdentity.data(), _localIOSocketIdentity.size()}, [](auto) {});
        onWrite();
#endif  // _WIN32
        if (_rawTCPConnectionFD.prepareReadBytes(this->_eventManager.get())) {
            onRead();
        }
    }
}

std::expected<void, MessageConnectionTCP::IOError> MessageConnectionTCP::tryReadOneMessage()
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

        auto res = _rawTCPConnectionFD.readBytes(readTo, remainingSize);
        if (res) {
            message._cursor += res.value();
        } else {
            switch (res.error()) {
                case RawTCPConnectionFD::IOError::Disconnected: {
                    return std::unexpected {IOError::Disconnected};
                }
                case RawTCPConnectionFD::IOError::Aborted: {
                    return std::unexpected {IOError::Aborted};
                }
                case RawTCPConnectionFD::IOError::Drained: {
                    return std::unexpected {IOError::Drained};
                }
            }
        }
    }
    return {};
}

// on Return, unexpected value shall be interpreted as this - 0 = close, other -> errno
std::expected<void, MessageConnectionTCP::IOError> MessageConnectionTCP::tryReadMessages()
{
    while (true) {
        auto res = tryReadOneMessage();
        if (!res) {
            return res;
        }
    }
}

void MessageConnectionTCP::updateReadOperation()
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

void MessageConnectionTCP::setRemoteIdentity() noexcept
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

void MessageConnectionTCP::onRead()
{
    if (_connFd == 0) {
        return;
    }

    auto maybeCloseConn = [this](IOError err) -> std::expected<void, IOError> {
        setRemoteIdentity();

        if (_remoteIOSocketIdentity) {
            updateReadOperation();
        }

        switch (err) {
            case IOError::Drained: return {};
            case IOError::Aborted: _disconnect = false; break;
            case IOError::Disconnected: _disconnect = true; break;
            case IOError::MessageTooLarge: _disconnect = true; break;
        }

        onClose();
        return std::unexpected {err};
    };

    auto res = _remoteIOSocketIdentity
                   .or_else([this, maybeCloseConn] {
                       auto _ = tryReadOneMessage()
                                    .or_else(maybeCloseConn)  //
                                    .and_then([this]() -> std::expected<void, IOError> {
                                        setRemoteIdentity();
                                        return {};
                                    });
                       return _remoteIOSocketIdentity;
                   })
                   .and_then([this, maybeCloseConn](const std::string&) -> std::optional<std::string> {
                       if (!_connFd) {
                           return _remoteIOSocketIdentity;
                       }
                       auto _ = tryReadMessages()
                                    .or_else(maybeCloseConn)  //
                                    .and_then([this]() -> std::expected<void, IOError> {
                                        updateReadOperation();
                                        return {};
                                    });
                       return _remoteIOSocketIdentity;
                   });
    if (!res) {
        return;
    }

    if (!_connFd) {
        return;
    }

    if (_rawTCPConnectionFD.prepareReadBytes(this->_eventManager.get())) {
        onRead();
    }
}

void MessageConnectionTCP::onWrite()
{
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

    if (res.error() == IOError::Disconnected) {
        _disconnect = true;
        onClose();
        return;
    }

    if (res.error() == IOError::Aborted) {
        onClose();
        return;
    }

    if (res.error() == IOError::Drained) {
        char* addr = nullptr;
        size_t len = 0;
        if (_sendCursor < HEADER_SIZE) {
            addr = (char*)(&_writeOperations.front()._header) + _sendCursor;
            len  = HEADER_SIZE - _sendCursor;
        } else {
            addr = (char*)_writeOperations.front()._payload.data() + (_sendCursor - HEADER_SIZE);
            len  = _writeOperations.front()._payload.len() - (_sendCursor - HEADER_SIZE);
        }

        auto [bytesSent, immediateExec] = _rawTCPConnectionFD.prepareWriteBytes(addr, len, _eventManager.get());
        _sendCursor += bytesSent;
        if (immediateExec) {
            onWrite();
            return;
        }
        return;
    }

    std::unreachable();
    // NOTE: This is unreachable
    // if (res.error() == IOError::MessageTooLarge) {}
}

void MessageConnectionTCP::onClose()
{
    if (_connFd) {
        _eventLoopThread->_eventLoop.removeFdFromLoop(_connFd);
        CloseAndZeroSocket(_connFd);
        auto& sock = _eventLoopThread->_identityToIOSocket.at(_localIOSocketIdentity);
        sock->onConnectionDisconnected(this, !_disconnect);
    }
};

std::expected<size_t, MessageConnectionTCP::IOError> MessageConnectionTCP::trySendQueuedMessages()
{
    using BufferType = std::pair<const unsigned char*, size_t>;
    std::vector<BufferType> buffers;
    buffers.reserve(_writeOperations.size());

    for (auto it = _writeOperations.begin(); it != _writeOperations.end(); ++it) {
        BufferType header;
        BufferType payload;

        if (it == _writeOperations.begin()) {
            if (_sendCursor < HEADER_SIZE) {
                header.first   = (unsigned char*)(&it->_header) + _sendCursor;
                header.second  = HEADER_SIZE - _sendCursor;
                payload.first  = (unsigned char*)(it->_payload.data());
                payload.second = it->_payload.len();
            } else {
                header.first   = nullptr;
                header.second  = 0;
                payload.first  = (unsigned char*)(it->_payload.data()) + (_sendCursor - HEADER_SIZE);
                payload.second = it->_payload.len() - (_sendCursor - HEADER_SIZE);
            }
        } else {
            header.first   = (unsigned char*)(&it->_header);
            header.second  = HEADER_SIZE;
            payload.first  = (unsigned char*)(it->_payload.data());
            payload.second = it->_payload.len();
        }
        buffers.emplace_back(header);
        buffers.emplace_back(payload);
    }

    auto res = _rawTCPConnectionFD.writeBytes(std::move(buffers));
    if (res) {
        return res.value();
    } else {
        switch (res.error()) {
            case RawTCPConnectionFD::IOError::Drained: {
                return std::unexpected {MessageConnectionTCP::IOError::Drained};
            }
            case RawTCPConnectionFD::IOError::Aborted: {
                return std::unexpected {MessageConnectionTCP::IOError::Aborted};
            }
            case RawTCPConnectionFD::IOError::Disconnected: {
                return std::unexpected {MessageConnectionTCP::IOError::Disconnected};
            }
            default: std::unreachable();
        }
    }
}

// TODO: There is a classic optimization that can (and should) be done. That is, we store
// prefix sum in each write operation, and perform binary search instead of linear search
// to find the first write operation we haven't complete. - gxu
void MessageConnectionTCP::updateWriteOperations(size_t n)
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

void MessageConnectionTCP::sendMessage(Message msg, SendMessageCallback onMessageSent)
{
    TcpWriteOperation writeOp(std::move(msg), std::move(onMessageSent));
    _writeOperations.emplace_back(std::move(writeOp));

    if (_connFd == 0) {
        return;
    }
    onWrite();
}

bool MessageConnectionTCP::recvMessage()
{
    if (_receivedReadOperations.empty() || _pendingRecvMessageCallbacks->empty() ||
        !isCompleteMessage(_receivedReadOperations.front())) {
        return false;
    }

    updateReadOperation();
    return true;
}

void MessageConnectionTCP::disconnect()
{
#ifdef __linux__
    _disconnect = true;
    shutdown(_connFd, SHUT_WR);
    onClose();
#endif
}

MessageConnectionTCP::~MessageConnectionTCP() noexcept
{
    if (_connFd != 0) {
        _eventLoopThread->_eventLoop.removeFdFromLoop(_connFd);

#ifdef __linux__
        shutdown(_connFd, SHUT_RD);

#endif  // __linux__
#ifdef _WIN32
        shutdown(_connFd, SD_BOTH);
#endif  // _WIN32

        CloseAndZeroSocket(_connFd);
    }

    std::ranges::for_each(_writeOperations, [](auto&& x) {
        x._callbackAfterCompleteWrite(std::unexpected {Error::ErrorCode::SendMessageRequestCouldNotComplete});
    });

    // TODO: What to do with this?
    // std::queue<std::vector<char>> _receivedMessages;
}

}  // namespace ymq
}  // namespace scaler

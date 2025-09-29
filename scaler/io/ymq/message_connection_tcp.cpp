
#include "scaler/io/ymq/message_connection_tcp.h"

#include <future>

#include "scaler/io/ymq/configuration.h"

#ifdef __linux__
#include <unistd.h>
#endif  // __linux__
#ifdef _WIN32
// clang-format off
#include <windows.h>
#include <winsock2.h>
#include <mswsock.h>
// clang-format on
#endif  // _WIN32

#include <algorithm>
#include <cerrno>
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
        // This probably need handle the addtwice problem
        this->_eventLoopThread->_eventLoop.addFdToLoop(_connFd, 0, nullptr);
        _writeOperations.emplace_back(
            Bytes {_localIOSocketIdentity.data(), _localIOSocketIdentity.size()}, [](auto) {});
        onWrite();
        const bool ok = ReadFile((HANDLE)(SOCKET)_connFd, nullptr, 0, nullptr, this->_eventManager.get());
        if (ok) {
            onRead();
            return;
        }
        const int lastError = GetLastError();
        if (lastError == ERROR_IO_PENDING) {
            return;
        }
        unrecoverableError({
            Error::ErrorCode::CoreBug,
            "Originated from",
            "ReadFile",
            "Errno is",
            lastError,
            "_connfd",
            _connFd,
        });
#endif  // _WIN32
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
            if (message._header >= LARGEST_PAYLOAD_SIZE) {
                return std::unexpected {IOError::MessageTooLarge};
            }
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

        int n = ::recv(_connFd, readTo, remainingSize, 0);
        if (n == 0) {
            return std::unexpected {IOError::Disconnected};
        } else if (n == -1) {
            const int myErrno = GetErrorCode();
#ifdef _WIN32
            if (myErrno == WSAEWOULDBLOCK) {
                return std::unexpected {IOError::Drained};
            }
            if (myErrno == WSAECONNRESET || myErrno == WSAENOTSOCK) {
                return std::unexpected {IOError::Aborted};
            } else {
                // NOTE: On Windows we don't have signals and weird IO Errors
                unrecoverableError({
                    Error::ErrorCode::CoreBug,
                    "Originated from",
                    "recv",
                    "Errno is",
                    myErrno,
                    "_connfd",
                    _connFd,
                    "readTo",
                    (void*)readTo,
                    "remainingSize",
                    remainingSize,
                });
            }
#endif  // _WIN32
#ifdef __linux__
            if (myErrno == ECONNRESET) {
                return std::unexpected {IOError::Aborted};
            }
            if (myErrno == EAGAIN || myErrno == EWOULDBLOCK) {
                return std::unexpected {IOError::Drained};
            } else {
                const int myErrno = errno;
                switch (myErrno) {
                    case EBADF:
                    case EISDIR:
                    case EINVAL:
                        unrecoverableError({
                            Error::ErrorCode::CoreBug,
                            "Originated from",
                            "read(2)",
                            "Errno is",
                            strerror(myErrno),
                            "_connfd",
                            _connFd,
                            "readTo",
                            (void*)readTo,
                            "remainingSize",
                            remainingSize,
                        });

                    case EINTR:
                        unrecoverableError({
                            Error::ErrorCode::SignalNotSupported,
                            "Originated from",
                            "read(2)",
                            "Errno is",
                            strerror(myErrno),
                        });

                    case EFAULT:
                    case EIO:
                    default:
                        unrecoverableError({
                            Error::ErrorCode::ConfigurationError,
                            "Originated from",
                            "read(2)",
                            "Errno is",
                            strerror(myErrno),
                        });
                }
            }
#endif  // __linux__
        } else {
            message._cursor += n;
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

#ifdef _WIN32
    const bool ok = ReadFile((HANDLE)(SOCKET)_connFd, nullptr, 0, nullptr, this->_eventManager.get());
    if (ok) {
        onRead();
        return;
    }
    const auto lastError = GetLastError();
    if (lastError == ERROR_IO_PENDING) {
        return;
    }
    unrecoverableError({
        Error::ErrorCode::CoreBug,
        "Originated from",
        "ReadFile",
        "Errno is",
        lastError,
        "_connfd",
        _connFd,
    });
#endif  // _WIN32
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

    if (res.error() == IOError::Aborted) {
        onClose();
        return;
    }

#ifdef _WIN32
    // NOTE: Precondition is the queue still has messages (perhaps a partial one).
    // We don't need to update the queue because trySendQueuedMessages is okay with a complete message in front.
    if (res.error() == IOError::Drained) {
        void* addr = nullptr;
        if (_sendCursor < HEADER_SIZE) {
            addr = (char*)(&_writeOperations.front()._header) + _sendCursor;
        } else {
            addr = (char*)_writeOperations.front()._payload.data() + _sendCursor - HEADER_SIZE;
        }
        ++_sendCursor;  // Next onWrite() will not be called until the asyncop complete

        const bool writeFileRes = WriteFile((HANDLE)(SOCKET)_connFd, addr, 1, nullptr, _eventManager.get());
        if (writeFileRes) {
            onWrite();
            return;
        }

        const auto lastError = GetLastError();
        if (lastError == ERROR_IO_PENDING) {
            return;
        }
        unrecoverableError({
            Error::ErrorCode::CoreBug,
            "Originated from",
            "WriteFile",
            "Errno is",
            lastError,
            "_connfd",
            _connFd,
        });
    }
#endif  // _WIN32
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
// typedef struct _WSABUF {
//     ULONG(same to sizet on x64 machine) len;     /* the length of the buffer */
//     _Field_size_bytes_(len) CHAR FAR *buf; /* the pointer to the buffer */
// } WSABUF, FAR * LPWSABUF;
#ifdef _WIN32
#define iovec    ::WSABUF
#define IOV_MAX  (1024)
#define iov_base buf
#define iov_len  len
#endif  // _WIN32

    std::vector<iovec> iovecs;
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
                iovPayload.iov_base = (char*)(it->_payload.data());
                iovPayload.iov_len  = it->_payload.len();
            } else {
                iovHeader.iov_base  = nullptr;
                iovHeader.iov_len   = 0;
                iovPayload.iov_base = (char*)(it->_payload.data()) + (_sendCursor - HEADER_SIZE);
                iovPayload.iov_len  = it->_payload.len() - (_sendCursor - HEADER_SIZE);
            }
        } else {
            iovHeader.iov_base  = (char*)(&it->_header);
            iovHeader.iov_len   = HEADER_SIZE;
            iovPayload.iov_base = (char*)(it->_payload.data());
            iovPayload.iov_len  = it->_payload.len();
        }

        iovecs.push_back(iovHeader);
        iovecs.push_back(iovPayload);
    }

    if (iovecs.empty()) {
        return 0;
    }

#ifdef _WIN32
    DWORD bytesSent {};
    const int sendToResult =
        WSASendTo(_connFd, iovecs.data(), iovecs.size(), &bytesSent, 0, nullptr, 0, nullptr, nullptr);
    if (sendToResult == 0) {
        return bytesSent;
    }
    const int myErrno = GetErrorCode();
    if (myErrno == WSAEWOULDBLOCK) {
        return std::unexpected {IOError::Drained};
    }
    if (myErrno == WSAESHUTDOWN || myErrno == WSAENOTCONN) {
        return std::unexpected {IOError::Aborted};
    }
    unrecoverableError({
        Error::ErrorCode::CoreBug,
        "Originated from",
        "WSASendTo",
        "Errno is",
        myErrno,
        "_connfd",
        _connFd,
        "iovecs.size()",
        iovecs.size(),
    });
#endif  // _WIN32

#ifdef __linux__
    struct msghdr msg {};
    msg.msg_iov    = iovecs.data();
    msg.msg_iovlen = iovecs.size();

    ssize_t bytesSent = ::sendmsg(_connFd, &msg, MSG_NOSIGNAL);
    if (bytesSent == -1) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return std::unexpected {IOError::Drained};
        } else {
            const int myErrno = errno;
            switch (myErrno) {
                case EAFNOSUPPORT:
                case EBADF:
                case EINVAL:
                case EMSGSIZE:
                case ENOTCONN:
                case ENOTSOCK:
                case EOPNOTSUPP:
                case ENAMETOOLONG:
                case ENOENT:
                case ENOTDIR:
                case ELOOP:
                case EDESTADDRREQ:
                case EHOSTUNREACH:
                case EISCONN:
                    unrecoverableError({
                        Error::ErrorCode::CoreBug,
                        "Originated from",
                        "sendmsg(2)",
                        "Errno is",
                        strerror(myErrno),
                        "_connfd",
                        _connFd,
                        "msg.msg_iovlen",
                        msg.msg_iovlen,
                    });
                    break;

                case ECONNRESET:
                case EPIPE: return std::unexpected {IOError::Aborted}; break;

                case EINTR:
                    unrecoverableError({
                        Error::ErrorCode::SignalNotSupported,
                        "Originated from",
                        "sendmsg(2)",
                        "Errno is",
                        strerror(myErrno),
                    });
                    break;

                case EIO:
                case EACCES:
                case ENETDOWN:
                case ENETUNREACH:
                case ENOBUFS:
                case ENOMEM:
                default:
                    unrecoverableError({
                        Error::ErrorCode::ConfigurationError,
                        "Originated from",
                        "sendmsg(2)",
                        "Errno is",
                        strerror(myErrno),
                    });
                    break;
            }
        }
    }

    return bytesSent;
#endif  // __linux__

#ifdef _WIN32
#undef iovec
#undef IOV_MAX
#undef iov_base
#undef iov_len
#endif  // _WIN32
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

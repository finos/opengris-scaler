#include "scaler/ymq/tcp_client.h"

#ifdef __linux__
#include <netinet/in.h>
#include <sys/socket.h>
#endif  // __linux__

#include <cerrno>
#include <chrono>
#include <functional>
#include <memory>

#include "scaler/error/error.h"
#include "scaler/ymq/event_loop_thread.h"
#include "scaler/ymq/event_manager.h"
#include "scaler/ymq/io_socket.h"
#include "scaler/ymq/message_connection_tcp.h"
#include "scaler/ymq/network_utils.h"
#include "scaler/ymq/timestamp.h"

namespace scaler {
namespace ymq {

void TcpClient::onCreated()
{
    assert(_connFd == 0);
    assert(_eventManager.get() != nullptr);
#ifdef __linux__
    int sockfd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, IPPROTO_TCP);

    if (sockfd == -1) {
        const int myErrno = errno;
        switch (myErrno) {
            case EACCES:
            case EAFNOSUPPORT:
            case EMFILE:
            case ENFILE:
            case ENOBUFS:
            case ENOMEM:
                unrecoverableError({
                    Error::ErrorCode::ConfigurationError,
                    "Originated from",
                    "socket(2)",
                    "Errno is",
                    strerror(myErrno),
                });
                break;

            case EINVAL:
            case EPROTONOSUPPORT:
            default:
                unrecoverableError({
                    Error::ErrorCode::CoreBug,
                    "Originated from",
                    "socket(2)",
                    "Errno is",
                    strerror(myErrno),
                });
                break;
        }
        return;
    }

    this->_connFd = sockfd;
    const int ret = connect(sockfd, (sockaddr*)&_remoteAddr, sizeof(_remoteAddr));
    if (ret >= 0) [[unlikely]] {
        std::string id                 = this->_localIOSocketIdentity;
        auto sock                      = this->_eventLoopThread->_identityToIOSocket.at(id);
        const bool responsibleForRetry = true;
        sock->onConnectionCreated(setNoDelay(sockfd), getLocalAddr(sockfd), getRemoteAddr(sockfd), responsibleForRetry);
        if (_retryTimes == 0) {
            _onConnectReturn({});
            _onConnectReturn = {};
        }
        return;
    }

    if (errno == EINPROGRESS) {
        _eventLoopThread->_eventLoop.addFdToLoop(sockfd, EPOLLOUT | EPOLLET, this->_eventManager.get());
        if (_retryTimes == 0) {
            _onConnectReturn(std::unexpected {Error::ErrorCode::InitialConnectFailedWithInProgress});
            _onConnectReturn = {};
        }
        return;
    }

    CloseAndZeroSocket(sockfd);

    const int myErrno = errno;
    switch (myErrno) {
        case EACCES:
        case EADDRNOTAVAIL:
        case EFAULT:
        case ENETUNREACH:
        case EPROTOTYPE:
        case ETIMEDOUT:
            unrecoverableError({
                Error::ErrorCode::ConfigurationError,
                "Originated from",
                "connect(2)",
                "Errno is",
                strerror(myErrno),
                "sockfd",
                sockfd,
            });
            break;

        case EINTR:
            unrecoverableError({
                Error::ErrorCode::SignalNotSupported,
                "Originated from",
                "connect(2)",
                "Errno is",
                strerror(myErrno),
                "sockfd",
                sockfd,
            });
            break;

        case EPERM:
        case EADDRINUSE:
        case EAFNOSUPPORT:
        case EAGAIN:
        case EALREADY:
        case EBADF:
        case EISCONN:
        case ENOTSOCK:
            unrecoverableError({
                Error::ErrorCode::CoreBug,
                "Originated from",
                "connect(2)",
                "Errno is",
                strerror(myErrno),
                "sockfd",
                sockfd,
            });
            break;

        case EINPROGRESS:
        case ECONNREFUSED:
        default: break;
    }
#endif  // __linux__
#ifdef _WIN32
    _connFd         = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    u_long nonblock = 1;
    ioctlsocket(_connFd, FIONBIO, &nonblock);
    DWORD res;
    GUID guid = WSAID_CONNECTEX;
    WSAIoctl(
        this->_connFd,
        SIO_GET_EXTENSION_FUNCTION_POINTER,
        (void*)&guid,
        sizeof(GUID),
        &_connectExFunc,
        sizeof(_connectExFunc),
        &res,
        0,
        0);
    if (!_connectExFunc) {
        unrecoverableError({
            Error::ErrorCode::CoreBug,
            "Originated from",
            "WSAIoctl",
            "Errno is",
            GetErrorCode(),
            "_connFd",
            _connFd,
        });
    }
    _eventLoopThread->_eventLoop.addFdToLoop(_connFd, 0, nullptr);

    sockaddr_in localAddr      = {};
    localAddr.sin_family       = AF_INET;
    const char ip4[]           = {127, 0, 0, 1};
    *(int*)&localAddr.sin_addr = *(int*)ip4;

    const int bindRes = bind(_connFd, (struct sockaddr*)&localAddr, sizeof(struct sockaddr_in));
    if (bindRes == -1) {
        unrecoverableError({
            Error::ErrorCode::ConfigurationError,
            "Originated from",
            "bind",
            "Errno is",
            GetErrorCode(),
            "_connFd",
            _connFd,
        });
    }

    const bool ok =
        _connectExFunc(_connFd, &_remoteAddr, sizeof(struct sockaddr), NULL, 0, NULL, this->_eventManager.get());
    if (ok) {
        unrecoverableError({
            Error::ErrorCode::CoreBug,
            "Originated from",
            "connectEx",
            "_connFd",
            _connFd,
        });
    }

    const int myErrno = GetErrorCode();
    if (myErrno == ERROR_IO_PENDING) {
        if (_retryTimes == 0) {
            _onConnectReturn(std::unexpected {Error::ErrorCode::InitialConnectFailedWithInProgress});
            _onConnectReturn = {};
        }
        return;
    }

    unrecoverableError({
        Error::ErrorCode::CoreBug,
        "Originated from",
        "connectEx",
        "Errno is",
        myErrno,
        "_connFd",
        _connFd,
    });
#endif  // _WIN32
}

TcpClient::TcpClient(
    std::shared_ptr<EventLoopThread> eventLoopThread,
    std::string localIOSocketIdentity,
    sockaddr remoteAddr,
    ConnectReturnCallback onConnectReturn,
    size_t maxRetryTimes) noexcept
    : _eventLoopThread(eventLoopThread)
    , _connected(false)
    , _onConnectReturn(std::move(onConnectReturn))
    , _connFd {}
    , _localIOSocketIdentity(std::move(localIOSocketIdentity))
    , _remoteAddr(std::move(remoteAddr))
    , _retryIdentifier {}
    , _eventManager(std::make_unique<EventManager>())
    , _retryTimes {}
    , _maxRetryTimes(maxRetryTimes)
#ifdef _WIN32
    , _connectExFunc {}
#endif  // _WIN32
{
    _eventManager->onRead  = [this] { this->onRead(); };
    _eventManager->onWrite = [this] { this->onWrite(); };
    _eventManager->onClose = [this] { this->onClose(); };
    _eventManager->onError = [this] { this->onError(); };
}

void TcpClient::onRead()
{
}

void TcpClient::onWrite()
{
#ifdef __linux__
    _eventLoopThread->_eventLoop.removeFdFromLoop(_connFd);
    int err {};
    socklen_t errLen {sizeof(err)};
    if (getsockopt(_connFd, SOL_SOCKET, SO_ERROR, &err, &errLen) < 0) {
        const int myErrno = errno;
        switch (myErrno) {
            case ENOPROTOOPT:
            case ENOBUFS:
            case EACCES:
                unrecoverableError({
                    Error::ErrorCode::ConfigurationError,
                    "Originated from",
                    "getsockopt(3)",
                    "Errno is",
                    strerror(myErrno),
                    "_connFd",
                    _connFd,
                });

            case ENOTSOCK:
            case EBADF:
            case EINVAL:
            default:
                unrecoverableError({
                    Error::ErrorCode::CoreBug,
                    "Originated from",
                    "getsockopt(3)",
                    "Errno is",
                    strerror(myErrno),
                    "_connFd",
                    _connFd,
                });
        }
    }

    if (err != 0) {
        if (err == ECONNREFUSED) {
            retry();
            return;
        }

        // Since connect(2) error has been checked previously
        unrecoverableError({
            Error::ErrorCode::CoreBug,
            "Originated from",
            "connect(2)",
            "Errno is",
            strerror(errno),
            "_connFd",
            _connFd,
        });
    }

    std::string id                 = this->_localIOSocketIdentity;
    auto sock                      = this->_eventLoopThread->_identityToIOSocket.at(id);
    const bool responsibleForRetry = true;
    sock->onConnectionCreated(setNoDelay(_connFd), getLocalAddr(_connFd), getRemoteAddr(_connFd), responsibleForRetry);

    _connFd    = 0;
    _connected = true;

    _eventLoopThread->_eventLoop.executeLater([sock] { sock->removeConnectedTcpClient(); });

#endif  // __linux__
#ifdef _WIN32
    const int iResult = setsockopt(_connFd, SOL_SOCKET, SO_UPDATE_CONNECT_CONTEXT, NULL, 0);
    if (iResult == -1) {
        CloseAndZeroSocket(_connFd);
        retry();
        return;
    }

    std::string id                 = this->_localIOSocketIdentity;
    auto sock                      = this->_eventLoopThread->_identityToIOSocket.at(id);
    const bool responsibleForRetry = true;
    sock->onConnectionCreated(setNoDelay(_connFd), getLocalAddr(_connFd), getRemoteAddr(_connFd), responsibleForRetry);

    _connFd    = 0;
    _connected = true;

    _eventLoopThread->_eventLoop.executeLater([sock] { sock->removeConnectedTcpClient(); });
#endif  // _WIN32
}

void TcpClient::retry()
{
    if (_retryTimes > _maxRetryTimes) {
        _logger.log(Logger::LoggingLevel::error, "Retried times has reached maximum: ", _maxRetryTimes);
        // exit(1);
        return;
    }

    _logger.log(Logger::LoggingLevel::debug, "Client retrying ", _retryTimes, " time(s)");
    CloseAndZeroSocket(_connFd);

    Timestamp now;
    auto at = now.createTimestampByOffsetDuration(std::chrono::seconds(2 << _retryTimes++));

    _retryIdentifier = _eventLoopThread->_eventLoop.executeAt(at, [this] { this->onCreated(); });
}

TcpClient::~TcpClient() noexcept
{
    if (_connFd) {
        _eventLoopThread->_eventLoop.removeFdFromLoop(_connFd);
        CloseAndZeroSocket(_connFd);
    }
    if (_retryTimes > 0) {
        _eventLoopThread->_eventLoop.cancelExecution(_retryIdentifier);
    }
    // TODO: Do we think this is an error? See TcpServer::~TcpServer for detail.
    if (_onConnectReturn) {
        _onConnectReturn({});
    }
}

}  // namespace ymq
}  // namespace scaler

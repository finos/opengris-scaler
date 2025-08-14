#include "scaler/io/ymq/tcp_client.h"

#include <netinet/in.h>
#include <sys/socket.h>

#include <cerrno>
#include <chrono>
#include <functional>
#include <memory>

#include "scaler/io/ymq/error.h"
#include "scaler/io/ymq/event_loop_thread.h"
#include "scaler/io/ymq/event_manager.h"
#include "scaler/io/ymq/io_socket.h"
#include "scaler/io/ymq/logging.h"
#include "scaler/io/ymq/message_connection_tcp.h"
#include "scaler/io/ymq/network_utils.h"
#include "scaler/io/ymq/timestamp.h"

namespace scaler {
namespace ymq {

void TcpClient::onCreated()
{
    int sockfd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
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
        }
        return;
    }

    if (errno == EINPROGRESS) {
        _eventLoopThread->_eventLoop.addFdToLoop(sockfd, EPOLLOUT | EPOLLET, this->_eventManager.get());
        if (_retryTimes == 0) {
            _onConnectReturn(std::unexpected {Error::ErrorCode::InitialConnectFailedWithInProgress});
        }
        return;
    }

    close(sockfd);

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
}

TcpClient::TcpClient(
    std::shared_ptr<EventLoopThread> eventLoopThread,
    std::string localIOSocketIdentity,
    sockaddr remoteAddr,
    ConnectReturnCallback onConnectReturn,
    size_t maxRetryTimes) noexcept
    : _eventLoopThread(eventLoopThread)
    , _localIOSocketIdentity(std::move(localIOSocketIdentity))
    , _remoteAddr(std::move(remoteAddr))
    , _eventManager(std::make_unique<EventManager>())
    , _connected(false)
    , _onConnectReturn(std::move(onConnectReturn))
    , _maxRetryTimes(maxRetryTimes)
    , _retryTimes {}
    , _retryIdentifier {}
{
    _eventManager->onRead  = [this] { this->onRead(); };
    _eventManager->onWrite = [this] { this->onWrite(); };
    _eventManager->onClose = [this] { this->onClose(); };
    _eventManager->onError = [this] { this->onError(); };
}

void TcpClient::onWrite()
{
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
}

void TcpClient::onRead()
{
}

void TcpClient::retry()
{
    if (_retryTimes > _maxRetryTimes) {
        log(LoggingLevel::error, "Retried times has reached maximum", _maxRetryTimes);
        panic();
    }

    log(LoggingLevel::debug, "Client retrying times", _retryTimes);
    close(_connFd);
    _connFd = 0;

    Timestamp now;
    auto at = now.createTimestampByOffsetDuration(std::chrono::seconds(2 << _retryTimes++));

    _retryIdentifier = _eventLoopThread->_eventLoop.executeAt(at, [this] { this->onCreated(); });
}

TcpClient::~TcpClient() noexcept
{
    if (_connFd) {
        _eventLoopThread->_eventLoop.removeFdFromLoop(_connFd);
        close(_connFd);
        _connFd = 0;
    }
    if (_retryTimes > 0) {
        _eventLoopThread->_eventLoop.cancelExecution(_retryIdentifier);
    }
}

}  // namespace ymq
}  // namespace scaler

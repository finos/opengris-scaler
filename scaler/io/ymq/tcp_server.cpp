#include "scaler/io/ymq/tcp_server.h"

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <expected>
#include <memory>

#include "scaler/io/ymq/error.h"
#include "scaler/io/ymq/event_loop_thread.h"
#include "scaler/io/ymq/event_manager.h"
#include "scaler/io/ymq/io_socket.h"
#include "scaler/io/ymq/logging.h"
#include "scaler/io/ymq/message_connection_tcp.h"
#include "scaler/io/ymq/network_utils.h"

namespace scaler {
namespace ymq {

int TcpServer::createAndBindSocket()
{
    int server_fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (server_fd == -1) {
        unrecoverableError({
            Error::ErrorCode::ConfigurationError,
            "Originated from",
            "socket(2)",
            "Errno is",
            strerror(errno),
            "_serverFd",
            _serverFd,
        });

        _onBindReturn(std::unexpected(Error {Error::ErrorCode::ConfigurationError}));
        return -1;
    }

    int optval = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval)) == -1) {
        log(LoggingLevel::error,
            "Originated from",
            "setsockopt(2)",
            "Errno is",
            strerror(errno)  // ,
        );

        close(server_fd);
        _onBindReturn(std::unexpected(Error {Error::ErrorCode::SetSockOptNonFatalFailure}));
        return -1;
    }

    if (bind(server_fd, &_addr, sizeof(_addr)) == -1) {
        close(server_fd);
        unrecoverableError({
            Error::ErrorCode::ConfigurationError,
            "Originated from",
            "bind(2)",
            "Errno is",
            strerror(errno),
            "server_fd",
            server_fd,
        });

        return -1;
    }

    if (listen(server_fd, SOMAXCONN) == -1) {
        close(server_fd);
        unrecoverableError({
            Error::ErrorCode::ConfigurationError,
            "Originated from",
            "listen(2)",
            "Errno is",
            strerror(errno),
            "server_fd",
            server_fd,
        });

        return -1;
    }

    return server_fd;
}

TcpServer::TcpServer(
    std::shared_ptr<EventLoopThread> eventLoopThread,
    std::string localIOSocketIdentity,
    sockaddr addr,
    BindReturnCallback onBindReturn) noexcept
    : _eventLoopThread(eventLoopThread)
    , _localIOSocketIdentity(std::move(localIOSocketIdentity))
    , _eventManager(std::make_unique<EventManager>())
    , _addr(std::move(addr))
    , _onBindReturn(std::move(onBindReturn))
    , _serverFd {}
{
    _eventManager->onRead  = [this] { this->onRead(); };
    _eventManager->onWrite = [this] { this->onWrite(); };
    _eventManager->onClose = [this] { this->onClose(); };
    _eventManager->onError = [this] { this->onError(); };
}

void TcpServer::onCreated()
{
    _serverFd = createAndBindSocket();
    if (_serverFd == -1) {
        _serverFd = 0;
        return;
    }
    _eventLoopThread->_eventLoop.addFdToLoop(_serverFd, EPOLLIN | EPOLLET, this->_eventManager.get());
    _onBindReturn({});
}

void TcpServer::onRead()
{
    while (true) {
        sockaddr remoteAddr {};
        socklen_t remoteAddrLen = sizeof(remoteAddr);

        int fd = accept4(_serverFd, &remoteAddr, &remoteAddrLen, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (fd < 0) {
            const int myErrno = errno;
            switch (myErrno) {
                // Not an error
                // case EWOULDBLOCK: // same as EAGAIN
                case EAGAIN:
                case ECONNABORTED: return;

                case ENOTSOCK:
                case EOPNOTSUPP:
                case EINVAL:
                case EBADF:
                    unrecoverableError({
                        Error::ErrorCode::CoreBug,
                        "Originated from",
                        "accept4(2)",
                        "Errno is",
                        strerror(myErrno),
                        "_serverFd",
                        _serverFd,
                    });
                    break;

                case EINTR:
                    unrecoverableError({
                        Error::ErrorCode::SignalNotSupported,
                        "Originated from",
                        "accept4(2)",
                        "Errno is",
                        strerror(myErrno),
                    });
                    break;

                // config
                case EMFILE:
                case ENFILE:
                case ENOBUFS:
                case ENOMEM:
                case EFAULT:
                case EPERM:
                case EPROTO:
                case ENOSR:
                case ESOCKTNOSUPPORT:
                case EPROTONOSUPPORT:
                case ETIMEDOUT:
                default:
                    unrecoverableError({
                        Error::ErrorCode::ConfigurationError,
                        "Originated from",
                        "accept4(2)",
                        "Errno is",
                        strerror(myErrno),
                    });
                    break;
            }
        }

        if (remoteAddrLen > sizeof(remoteAddr)) {
            unrecoverableError({
                Error::ErrorCode::IPv6NotSupported,
                "Originated from",
                "accept4(2)",
                "remoteAddrLen",
                remoteAddrLen,
                "sizeof(remoteAddr)",
                sizeof(remoteAddr),
            });
        }

        std::string id = this->_localIOSocketIdentity;
        auto sock      = this->_eventLoopThread->_identityToIOSocket.at(id);
        sock->onConnectionCreated(setNoDelay(fd), getLocalAddr(fd), remoteAddr, false);
    }
}

TcpServer::~TcpServer() noexcept
{
    if (_serverFd != 0) {
        _eventLoopThread->_eventLoop.removeFdFromLoop(_serverFd);
        close(_serverFd);
    }
}

}  // namespace ymq
}  // namespace scaler

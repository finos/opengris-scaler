#include "scaler/io/ymq/tcp_server.h"

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <memory>

#include "scaler/io/ymq/configuration.h"
#include "scaler/io/ymq/event_loop_thread.h"
#include "scaler/io/ymq/event_manager.h"
#include "scaler/io/ymq/io_socket.h"
#include "scaler/io/ymq/message_connection_tcp.h"
#include "scaler/io/ymq/network_utils.h"

namespace scaler {
namespace ymq {

int TcpServer::createAndBindSocket() {
    int server_fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (server_fd == -1) {
        perror("socket");
        _onBindReturn(errno);
        return -1;
    }

    int optval = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval)) == -1) {
        perror("setsockopt");
        close(server_fd);
        _onBindReturn(errno);
        return -1;
    }

    if (bind(server_fd, &_addr, sizeof(_addr)) == -1) {
        perror("bind");
        close(server_fd);
        _onBindReturn(errno);
        return -1;
    }

    if (listen(server_fd, SOMAXCONN) == -1) {
        perror("listen");
        close(server_fd);
        _onBindReturn(errno);
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
    , _serverFd {} {
    _eventManager->onRead  = [this] { this->onRead(); };
    _eventManager->onWrite = [this] { this->onWrite(); };
    _eventManager->onClose = [this] { this->onClose(); };
    _eventManager->onError = [this] { this->onError(); };
}

void TcpServer::onCreated() {
    _serverFd = createAndBindSocket();
    if (_serverFd == -1) {
        _serverFd = 0;
        return;
    }
    auto res = _eventLoopThread->_eventLoop.addFdToLoop(_serverFd, EPOLLIN | EPOLLET, this->_eventManager.get());
    if (res == 0) {
        _onBindReturn(0);
    } else {
        close(_serverFd);
        _serverFd = 0;
        _onBindReturn(res);
    }
}

void TcpServer::onRead() {
    while (true) {
        sockaddr remoteAddr {};
        socklen_t remoteAddrLen = sizeof(remoteAddr);

        int fd = accept4(_serverFd, &remoteAddr, &remoteAddrLen, SOCK_NONBLOCK | SOCK_CLOEXEC);
        // TODO: Change this to a user callback
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
            fprintf(stderr, "Are you using IPv6? This is probably not supported as of now.\n");
            exit(-1);
        }

        std::string id = this->_localIOSocketIdentity;
        auto sock      = this->_eventLoopThread->_identityToIOSocket.at(id);
        sock->onConnectionCreated(setNoDelay(fd), getLocalAddr(fd), remoteAddr, false);
    }
}

TcpServer::~TcpServer() noexcept {
    if (_serverFd != 0) {
        _eventLoopThread->_eventLoop.removeFdFromLoop(_serverFd);
        close(_serverFd);
    }
}

}  // namespace ymq
}  // namespace scaler

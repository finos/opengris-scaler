#include "scaler/ymq/tcp_server.h"

#include <expected>
#include <memory>

#include "scaler/error/error.h"
#include "scaler/ymq/event_loop_thread.h"
#include "scaler/ymq/event_manager.h"
#include "scaler/ymq/internal/defs.h"  // system compatible header
#include "scaler/ymq/io_socket.h"
#include "scaler/ymq/message_connection_tcp.h"
#include "scaler/ymq/network_utils.h"

namespace scaler {
namespace ymq {

#ifdef _WIN32
void TcpServer::prepareAcceptSocket()
{
    _newConn = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (_newConn == INVALID_SOCKET) {
        const int myErrno = GetErrorCode();
        switch (myErrno) {
            case WSAENOBUFS:
            case WSAEMFILE:
            case WSAENETDOWN:
            case WSAEPROVIDERFAILEDINIT:
                unrecoverableError({
                    Error::ErrorCode::ConfigurationError,
                    "Originated from",
                    "socket(2)",
                    "Errno is",
                    strerror(myErrno),
                });
                break;
            case WSANOTINITIALISED:
            case WSAEINPROGRESS:
            case WSAEAFNOSUPPORT:
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
    }

    // TODO: Think about RawSocket abstraction that implements read/write/accept/connect/etc.
    DWORD bytesReturned                 = 0;
    const size_t requiredRedundantSpace = 16;
    if (!_acceptExFunc(
            _serverFd,
            _newConn,
            _buffer,
            0,
            sizeof(sockaddr_in) + requiredRedundantSpace,
            sizeof(sockaddr_in) + requiredRedundantSpace,
            &bytesReturned,
            _eventManager.get())) {
        const int myErrno = GetErrorCode();
        if (myErrno != ERROR_IO_PENDING) {
            CloseAndZeroSocket(_newConn);
            unrecoverableError({
                Error::ErrorCode::CoreBug,
                "Originated from",
                "AcceptEx",
                "Errno is",
                strerror(myErrno),
                "_serverFd",
                _serverFd,
            });
        }
        return;
    }
    // acceptEx never succeed.
}
#endif  // _WIN32

int TcpServer::createAndBindSocket()
{
#ifdef __linux__
    int server_fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, IPPROTO_TCP);
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

        // _onBindReturn(std::unexpected(Error {Error::ErrorCode::ConfigurationError}));
        return -1;
    }
#endif  // __linux__
#ifdef _WIN32
    auto server_fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (server_fd == -1) {
        const int myErrno = GetErrorCode();
        switch (myErrno) {
            case WSAENOBUFS:
            case WSAEMFILE:
            case WSAENETDOWN:
            case WSAEPROVIDERFAILEDINIT:
                unrecoverableError({
                    Error::ErrorCode::ConfigurationError,
                    "Originated from",
                    "socket(2)",
                    "Errno is",
                    strerror(myErrno),
                });
                break;
            case WSANOTINITIALISED:
            case WSAEINPROGRESS:
            case WSAEAFNOSUPPORT:
            default:
                unrecoverableError({
                    Error::ErrorCode::CoreBug,
                    "Originated from",
                    "socket(2)",
                    "Errno is",
                    myErrno,
                });
                break;
        }
        return -1;
    }
    unsigned long turnOnNonBlocking = 1;
    ioctlsocket(server_fd, FIONBIO, &turnOnNonBlocking);
#endif  // _WIN32

    int optval = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, (const char*)&optval, sizeof(optval)) == -1) {
        _logger.log(
            Logger::LoggingLevel::error,
            "Originated from",
            "setsockopt(2)",
            "Errno is",
            strerror(GetErrorCode())  // ,
        );
        CloseAndZeroSocket(server_fd);
        _onBindReturn(std::unexpected(Error {Error::ErrorCode::SetSockOptNonFatalFailure}));
        _onBindReturn = {};
        return -1;
    }

    if (bind(server_fd, &_addr, sizeof(_addr)) == -1) {
        CloseAndZeroSocket(server_fd);
        unrecoverableError({
            Error::ErrorCode::ConfigurationError,
            "Originated from",
            "bind(2)",
            "Errno is",
            strerror(GetErrorCode()),
            "server_fd",
            server_fd,
        });

        return -1;
    }

    if (listen(server_fd, SOMAXCONN) == -1) {
        CloseAndZeroSocket(server_fd);
        unrecoverableError({
            Error::ErrorCode::ConfigurationError,
            "Originated from",
            "listen(2)",
            "Errno is",
            strerror(GetErrorCode()),
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
    , _onBindReturn(std::move(onBindReturn))
    , _serverFd {}
    , _addr(std::move(addr))
    , _localIOSocketIdentity(std::move(localIOSocketIdentity))
    , _eventManager(std::make_unique<EventManager>())
#ifdef _WIN32
    , _acceptExFunc {}
    , _newConn {}
    , _buffer {}
#endif  // _WIN32
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
#ifdef __linux__
    _eventLoopThread->_eventLoop.addFdToLoop(_serverFd, EPOLLIN | EPOLLET, this->_eventManager.get());
#endif  // __linux__
#ifdef _WIN32
    // Events and EventManager are not used here.
    _eventLoopThread->_eventLoop.addFdToLoop(_serverFd, 0, nullptr);
    // Retrieve AcceptEx pointer once
    GUID guidAcceptEx   = WSAID_ACCEPTEX;
    DWORD bytesReturned = 0;
    if (WSAIoctl(
            _serverFd,
            SIO_GET_EXTENSION_FUNCTION_POINTER,
            &guidAcceptEx,
            sizeof(guidAcceptEx),
            &_acceptExFunc,
            sizeof(_acceptExFunc),
            &bytesReturned,
            nullptr,
            nullptr) == SOCKET_ERROR) {
        unrecoverableError({
            Error::ErrorCode::CoreBug,
            "Originated from",
            "WSAIoctl",
            "Errno is",
            strerror(GetErrorCode()),
            "_serverFd",
            _serverFd,
        });
    }
    prepareAcceptSocket();
#endif  // _WIN32

    _onBindReturn({});
    _onBindReturn = {};
}

void TcpServer::onRead()
{
#ifdef __linux__
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
#endif
#ifdef _WIN32
    if (setsockopt(
            _newConn, SOL_SOCKET, SO_UPDATE_ACCEPT_CONTEXT, reinterpret_cast<char*>(&_serverFd), sizeof(_serverFd)) ==
        SOCKET_ERROR) {
        CloseAndZeroSocket(_serverFd);
        CloseAndZeroSocket(_newConn);
        unrecoverableError({
            Error::ErrorCode::ConfigurationError,
            "Originated from",
            "setsockopt(SO_UPDATE_ACCEPT_CONTEXT)",
            "Errno is",
            strerror(GetErrorCode()),
            "_serverFd",
            _serverFd,
        });
    }

    unsigned long mode = 1;
    if (ioctlsocket(_newConn, FIONBIO, &mode) == SOCKET_ERROR) {
        const int myErrno = GetErrorCode();
        switch (myErrno) {
            case WSANOTINITIALISED:
            case WSAEINPROGRESS:
            case WSAENOTSOCK:
                unrecoverableError({
                    Error::ErrorCode::CoreBug,
                    "Originated from",
                    "ioctlsocket(FIONBIO)",
                    "Errno is",
                    strerror(myErrno),
                    "_newConn",
                    _newConn,
                });

            case WSAENETDOWN:
            case WSAEFAULT:
            default:
                unrecoverableError({
                    Error::ErrorCode::ConfigurationError,
                    "Originated from",
                    "ioctlsocket(FIONBIO)",
                    "Errno is",
                    strerror(myErrno),
                    "_newConn",
                    _newConn,
                });
                break;
        }
    }

    const auto& id = this->_localIOSocketIdentity;
    auto& sock     = this->_eventLoopThread->_identityToIOSocket.at(id);
    sock->onConnectionCreated(setNoDelay(_newConn), getLocalAddr(_newConn), getRemoteAddr(_newConn), false);
    prepareAcceptSocket();
#endif  // _WIN32
}

TcpServer::~TcpServer() noexcept
{
    if (_serverFd != 0) {
        _eventLoopThread->_eventLoop.removeFdFromLoop(_serverFd);
        CloseAndZeroSocket(_serverFd);
    }
    // TODO: Do we think this is an error? In extreme cases:
    // bindTo(...);
    // removeIOSocket(...);
    // Below callback may not be called.
    if (_onBindReturn) {
        _onBindReturn({});
    }
}

}  // namespace ymq
}  // namespace scaler

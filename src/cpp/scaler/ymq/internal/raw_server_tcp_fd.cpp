
#include "scaler/ymq/internal/raw_server_tcp_fd.h"

#include <utility>  // std::move

#include "scaler/error/error.h"
#include "scaler/ymq/internal/defs.h"
#include "scaler/ymq/network_utils.h"

namespace scaler {
namespace ymq {

RawServerTCPFD::RawServerTCPFD(sockaddr addr)
{
    _serverFD = {};
    _addr     = std::move(addr);

#ifdef _WIN32
    _newConn      = {};
    _acceptExFunc = {};
    memset(_buffer, 0, sizeof(_buffer));
#endif  // _WIN32

#ifdef __linux__
    _serverFD = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, IPPROTO_TCP);
    if ((int)_serverFD == -1) {
        unrecoverableError({
            Error::ErrorCode::ConfigurationError,
            "Originated from",
            "socket(2)",
            "Errno is",
            strerror(errno),
            "_serverFD",
            _serverFD,
        });

        return;
    }
#endif  // __linux__
#ifdef _WIN32
    _serverFD = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (_serverFD == -1) {
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
        return;
    }
    unsigned long turnOnNonBlocking = 1;
    ioctlsocket(_serverFD, FIONBIO, &turnOnNonBlocking);

    GUID guidAcceptEx   = WSAID_ACCEPTEX;
    DWORD bytesReturned = 0;
    if (WSAIoctl(
            _serverFD,
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
            "_serverFD",
            _serverFD,
        });
    }
#endif  // _WIN32
}

RawServerTCPFD::~RawServerTCPFD()
{
#ifdef _WIN32
    if (_newConn) {
        CloseAndZeroSocket(_newConn);
    }
#endif
    if (_serverFD) {
#ifdef _WIN32
        CancelIoEx((HANDLE)_serverFD, nullptr);
#endif
        CloseAndZeroSocket(_serverFD);
    }
}

bool RawServerTCPFD::setReuseAddress()
{
    int optval = 1;
    if (setsockopt(_serverFD, SOL_SOCKET, SO_REUSEADDR, (const char*)&optval, sizeof(optval)) == -1) {
        CloseAndZeroSocket(_serverFD);
        return false;
    }
    return true;
}

void RawServerTCPFD::bindAndListen()
{
    if (bind(_serverFD, &_addr, sizeof(_addr)) == -1) {
        CloseAndZeroSocket(_serverFD);
        unrecoverableError({
            Error::ErrorCode::ConfigurationError,
            "Originated from",
            "bind(2)",
            "Errno is",
            strerror(GetErrorCode()),
            "_serverFD",
            _serverFD,
        });

        return;
    }

    if (listen(_serverFD, SOMAXCONN) == -1) {
        CloseAndZeroSocket(_serverFD);
        unrecoverableError({
            Error::ErrorCode::ConfigurationError,
            "Originated from",
            "listen(2)",
            "Errno is",
            strerror(GetErrorCode()),
            "_serverFD",
            _serverFD,
        });

        return;
    }
}

void RawServerTCPFD::prepareAcceptSocket(void* notifyHandle)
{
#ifdef _WIN32
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
            _serverFD,
            _newConn,
            _buffer,
            0,
            sizeof(sockaddr_in) + requiredRedundantSpace,
            sizeof(sockaddr_in) + requiredRedundantSpace,
            &bytesReturned,
            (LPOVERLAPPED)notifyHandle)) {
        const int myErrno = GetErrorCode();
        if (myErrno != ERROR_IO_PENDING) {
            CloseAndZeroSocket(_newConn);
            unrecoverableError({
                Error::ErrorCode::CoreBug,
                "Originated from",
                "AcceptEx",
                "Errno is",
                strerror(myErrno),
                "_serverFD",
                _serverFD,
            });
        }
        return;
    }
    // acceptEx never succeed.
#endif  // _WIN32
}

std::vector<std::pair<uint64_t, sockaddr>> RawServerTCPFD::getNewConns()
{
    std::vector<std::pair<uint64_t, sockaddr>> res;
#ifdef __linux__
    while (true) {
        sockaddr remoteAddr {};
        socklen_t remoteAddrLen = sizeof(remoteAddr);

        int fd = accept4(_serverFD, &remoteAddr, &remoteAddrLen, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (fd < 0) {
            const int myErrno = errno;
            switch (myErrno) {
                // Not an error
                // case EWOULDBLOCK: // same as EAGAIN
                case EAGAIN:
                case ECONNABORTED: return res;

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
                        "_serverFD",
                        _serverFD,
                    });

                case EINTR:
                    unrecoverableError({
                        Error::ErrorCode::SignalNotSupported,
                        "Originated from",
                        "accept4(2)",
                        "Errno is",
                        strerror(myErrno),
                    });

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
        res.push_back({fd, remoteAddr});
    }
#endif

#ifdef _WIN32
    if (setsockopt(
            _newConn, SOL_SOCKET, SO_UPDATE_ACCEPT_CONTEXT, reinterpret_cast<char*>(&_serverFD), sizeof(_serverFD)) ==
        SOCKET_ERROR) {
        CloseAndZeroSocket(_serverFD);
        CloseAndZeroSocket(_newConn);
        unrecoverableError({
            Error::ErrorCode::ConfigurationError,
            "Originated from",
            "setsockopt(SO_UPDATE_ACCEPT_CONTEXT)",
            "Errno is",
            strerror(GetErrorCode()),
            "_serverFD",
            _serverFD,
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
    res.push_back({_newConn, getRemoteAddr(_newConn)});

    _newConn = 0;  // This _newConn will be handled by connection class

    return res;
#endif
}

void RawServerTCPFD::destroy()
{
#ifdef _WIN32
    if (_serverFD) {
        CancelIoEx((HANDLE)_serverFD, nullptr);
    }
#endif
    if (_serverFD) {
        CloseAndZeroSocket(_serverFD);
    }
}

}  // namespace ymq
}  // namespace scaler

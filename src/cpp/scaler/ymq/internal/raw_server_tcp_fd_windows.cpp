#ifdef _WIN32

#include <utility>  // std::move

#include "scaler/error/error.h"
#include "scaler/ymq/internal/defs.h"
#include "scaler/ymq/internal/network_utils.h"
#include "scaler/ymq/internal/raw_server_tcp_fd.h"
#include "scaler/ymq/network_utils.h"

namespace scaler {
namespace ymq {

RawServerTCPFD::RawServerTCPFD(sockaddr addr)
{
    _serverFD = {};
    _addr     = std::move(addr);

    _newConn      = {};
    _acceptExFunc = {};
    memset(_buffer, 0, sizeof(_buffer));

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
}

bool RawServerTCPFD::setReuseAddress()
{
    if (::scaler::ymq::setReuseAddress(_serverFD)) {
        return true;
    } else {
        CloseAndZeroSocket(_serverFD);
        return false;
    }
}

void RawServerTCPFD::bindAndListen()
{
    if (bind(_serverFD, &_addr, sizeof(_addr)) == -1) {
        const auto serverFD = _serverFD;
        CloseAndZeroSocket(_serverFD);
        unrecoverableError({
            Error::ErrorCode::ConfigurationError,
            "Originated from",
            "bind(2)",
            "Errno is",
            strerror(GetErrorCode()),
            "_serverFD",
            serverFD,
        });

        return;
    }

    if (listen(_serverFD, SOMAXCONN) == -1) {
        const auto serverFD = _serverFD;
        CloseAndZeroSocket(_serverFD);
        unrecoverableError({
            Error::ErrorCode::ConfigurationError,
            "Originated from",
            "listen(2)",
            "Errno is",
            strerror(GetErrorCode()),
            "_serverFD",
            serverFD,
        });

        return;
    }
}

RawServerTCPFD::~RawServerTCPFD()
{
    if (_newConn) {
        CloseAndZeroSocket(_newConn);
    }
    if (_serverFD) {
        CancelIoEx((HANDLE)_serverFD, nullptr);
        CloseAndZeroSocket(_serverFD);
    }
}

void RawServerTCPFD::prepareAcceptSocket(void* notifyHandle)
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
}

std::vector<std::pair<uint64_t, sockaddr>> RawServerTCPFD::getNewConns()
{
    std::vector<std::pair<uint64_t, sockaddr>> res;

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
}

void RawServerTCPFD::destroy()
{
    if (_serverFD) {
        CancelIoEx((HANDLE)_serverFD, nullptr);
    }
    if (_serverFD) {
        CloseAndZeroSocket(_serverFD);
    }
}

}  // namespace ymq
}  // namespace scaler

#endif

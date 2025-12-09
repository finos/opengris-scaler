#ifdef _WIN32

#include <utility>  // std::move

#include "scaler/error/error.h"
#include "scaler/ymq/internal/network_utils.h"
#include "scaler/ymq/internal/raw_stream_server_handle.h"

// clang-format off
#define NOMINMAX
#include <windows.h>
#include <winsock2.h>
#include <mswsock.h>
#include <ws2tcpip.h> // inet_pton
// clang-format on

namespace scaler {
namespace ymq {

struct RawStreamServerHandle::Impl {
    uint64_t _serverFD;
    SocketAddress _addr;
    uint64_t _newConn;
    LPFN_ACCEPTEX _acceptExFunc;
    char _buffer[128];
};

uint64_t RawStreamServerHandle::nativeHandle()
{
    return _impl->_serverFD;
}

RawStreamServerHandle::RawStreamServerHandle(SocketAddress addr): _impl(std::make_unique<RawStreamServerHandle::Impl>())
{
    _impl->_serverFD = {};
    _impl->_addr     = std::move(addr);

    _impl->_newConn      = {};
    _impl->_acceptExFunc = {};
    memset(_impl->_buffer, 0, sizeof(_impl->_buffer));
    if (_impl->_addr.nativeHandleType() == SocketAddress::Type::IPC) {
        unrecoverableError({
            Error::ErrorCode::IPCOnWinNotSupported,
        });
    }

    _impl->_serverFD = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (_impl->_serverFD == -1) {
        const int myErrno = WSAGetLastError();
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
    ioctlsocket(_impl->_serverFD, FIONBIO, &turnOnNonBlocking);

    GUID guidAcceptEx   = WSAID_ACCEPTEX;
    DWORD bytesReturned = 0;
    if (WSAIoctl(
            _impl->_serverFD,
            SIO_GET_EXTENSION_FUNCTION_POINTER,
            &guidAcceptEx,
            sizeof(guidAcceptEx),
            &_impl->_acceptExFunc,
            sizeof(_impl->_acceptExFunc),
            &bytesReturned,
            nullptr,
            nullptr) == SOCKET_ERROR) {
        unrecoverableError({
            Error::ErrorCode::CoreBug,
            "Originated from",
            "WSAIoctl",
            "Errno is",
            strerror(WSAGetLastError()),
            "_impl->_serverFD",
            _impl->_serverFD,
        });
    }
}

bool RawStreamServerHandle::setReuseAddress()
{
    int optval = 1;
    if (setsockopt(_impl->_serverFD, SOL_SOCKET, SO_REUSEADDR, (const char*)&optval, sizeof(optval)) != -1) {
        return true;
    } else {
        closeAndZeroSocket(&_impl->_serverFD);
        return false;
    }
}

void RawStreamServerHandle::bindAndListen()
{
    if (bind(_impl->_serverFD, _impl->_addr.nativeHandle(), _impl->_addr.nativeHandleLen()) == -1) {
        const auto serverFD = _impl->_serverFD;
        closeAndZeroSocket(&_impl->_serverFD);
        unrecoverableError({
            Error::ErrorCode::ConfigurationError,
            "Originated from",
            "bind(2)",
            "Errno is",
            strerror(WSAGetLastError()),
            "_impl->_serverFD",
            serverFD,
        });

        return;
    }

    if (listen(_impl->_serverFD, SOMAXCONN) == -1) {
        const auto serverFD = _impl->_serverFD;
        closeAndZeroSocket(&_impl->_serverFD);
        unrecoverableError({
            Error::ErrorCode::ConfigurationError,
            "Originated from",
            "listen(2)",
            "Errno is",
            strerror(WSAGetLastError()),
            "_impl->_serverFD",
            serverFD,
        });

        return;
    }
}

RawStreamServerHandle::~RawStreamServerHandle()
{
    if (_impl->_newConn) {
        closeAndZeroSocket(&_impl->_newConn);
    }
    if (_impl->_serverFD) {
        CancelIoEx((HANDLE)_impl->_serverFD, nullptr);
        closeAndZeroSocket(&_impl->_serverFD);
    }
}

void RawStreamServerHandle::prepareAcceptSocket(void* notifyHandle)
{
    _impl->_newConn = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (_impl->_newConn == INVALID_SOCKET) {
        const int myErrno = WSAGetLastError();
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
    if (!_impl->_acceptExFunc(
            _impl->_serverFD,
            _impl->_newConn,
            _impl->_buffer,
            0,
            sizeof(sockaddr_in) + requiredRedundantSpace,
            sizeof(sockaddr_in) + requiredRedundantSpace,
            &bytesReturned,
            (LPOVERLAPPED)notifyHandle)) {
        const int myErrno = WSAGetLastError();
        if (myErrno != ERROR_IO_PENDING) {
            closeAndZeroSocket(&_impl->_newConn);
            unrecoverableError({
                Error::ErrorCode::CoreBug,
                "Originated from",
                "AcceptEx",
                "Errno is",
                strerror(myErrno),
                "_impl->_serverFD",
                _impl->_serverFD,
            });
        }
        return;
    }
    // acceptEx never succeed.
}

std::vector<std::pair<uint64_t, SocketAddress>> RawStreamServerHandle::getNewConns()
{
    std::vector<std::pair<uint64_t, SocketAddress>> res;

    if (setsockopt(
            _impl->_newConn,
            SOL_SOCKET,
            SO_UPDATE_ACCEPT_CONTEXT,
            reinterpret_cast<char*>(&_impl->_serverFD),
            sizeof(_impl->_serverFD)) == SOCKET_ERROR) {
        closeAndZeroSocket(&_impl->_serverFD);
        closeAndZeroSocket(&_impl->_newConn);
        unrecoverableError({
            Error::ErrorCode::ConfigurationError,
            "Originated from",
            "setsockopt(SO_UPDATE_ACCEPT_CONTEXT)",
            "Errno is",
            strerror(WSAGetLastError()),
            "_impl->_serverFD",
            _impl->_serverFD,
        });
    }

    unsigned long mode = 1;
    if (ioctlsocket(_impl->_newConn, FIONBIO, &mode) == SOCKET_ERROR) {
        const int myErrno = WSAGetLastError();
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
                    "_impl->_newConn",
                    _impl->_newConn,
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
                    "_impl->_newConn",
                    _impl->_newConn,
                });
                break;
        }
    }
    res.push_back({_impl->_newConn, getRemoteAddr(_impl->_newConn)});
    _impl->_newConn = 0;  // This _impl->_newConn will be handled by connection class

    return res;
}

void RawStreamServerHandle::destroy()
{
    if (_impl->_serverFD) {
        CancelIoEx((HANDLE)_impl->_serverFD, nullptr);
    }
    if (_impl->_serverFD) {
        closeAndZeroSocket(&_impl->_serverFD);
    }
}

}  // namespace ymq
}  // namespace scaler

#endif

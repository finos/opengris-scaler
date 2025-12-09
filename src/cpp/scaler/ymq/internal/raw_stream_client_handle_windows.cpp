#ifdef _WIN32
#include "scaler/error/error.h"
#include "scaler/ymq/internal/network_utils.h"
#include "scaler/ymq/internal/raw_stream_client_handle.h"

// clang-format off
#define NOMINMAX
#include <windows.h>
#include <winsock2.h>
#include <mswsock.h>
#include <ws2tcpip.h> // inet_pton
// clang-format on

namespace scaler {
namespace ymq {

struct RawStreamClientHandle::Impl {
    uint64_t _clientFD;
    SocketAddress _remoteAddr;
    LPFN_CONNECTEX _connectExFunc;
};

uint64_t RawStreamClientHandle::nativeHandle()
{
    return _impl->_clientFD;
}

bool RawStreamClientHandle::isNetworkFD() const noexcept
{
    return _impl->_remoteAddr.nativeHandleType() == SocketAddress::Type::TCP;
}

RawStreamClientHandle::RawStreamClientHandle(SocketAddress remoteAddr)
    : _impl(std::make_unique<RawStreamClientHandle::Impl>())
{
    _impl->_clientFD   = 0;
    _impl->_remoteAddr = std::move(remoteAddr);

    _impl->_connectExFunc = {};
    if (_impl->_remoteAddr.nativeHandleType() == SocketAddress::Type::IPC) {
        unrecoverableError({
            Error::ErrorCode::IPCOnWinNotSupported,
        });
    }

    auto tmp = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    DWORD res;
    GUID guid = WSAID_CONNECTEX;
    WSAIoctl(
        tmp,
        SIO_GET_EXTENSION_FUNCTION_POINTER,
        (void*)&guid,
        sizeof(GUID),
        &_impl->_connectExFunc,
        sizeof(_impl->_connectExFunc),
        &res,
        0,
        0);
    closesocket(tmp);
    if (!_impl->_connectExFunc) {
        unrecoverableError({
            Error::ErrorCode::CoreBug,
            "Originated from",
            "WSAIoctl",
            "Errno is",
            WSAGetLastError(),
            "_impl->_connectExFunc",
            (void*)_impl->_connectExFunc,
        });
    }
}

void RawStreamClientHandle::create()
{
    switch (_impl->_remoteAddr.nativeHandleType()) {
        case SocketAddress::Type::TCP: _impl->_clientFD = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP); break;
        case SocketAddress::Type::IPC: _impl->_clientFD = socket(AF_UNIX, SOCK_STREAM, 0); break;
        default: std::unreachable();
    }
    if (_impl->_clientFD == -1) {
        unrecoverableError({
            Error::ErrorCode::CoreBug,
            "Originated from",
            "socket(2)",
            "Errno is",
            strerror(WSAGetLastError()),
        });
    }
    u_long nonblock = 1;
    ioctlsocket(_impl->_clientFD, FIONBIO, &nonblock);
}

bool RawStreamClientHandle::prepConnect(void* notifyHandle)
{
    sockaddr_in localAddr      = {};
    localAddr.sin_family       = AF_INET;
    const char ip4[]           = {127, 0, 0, 1};
    *(int*)&localAddr.sin_addr = *(int*)ip4;

    const int bindRes = bind(_impl->_clientFD, (struct sockaddr*)&localAddr, _impl->_remoteAddr.nativeHandleLen());
    if (bindRes == -1) {
        unrecoverableError({
            Error::ErrorCode::ConfigurationError,
            "Originated from",
            "bind",
            "Errno is",
            WSAGetLastError(),
            "_impl->_clientFD",
            _impl->_clientFD,
        });
    }

    const bool ok = _impl->_connectExFunc(
        _impl->_clientFD,
        _impl->_remoteAddr.nativeHandle(),
        _impl->_remoteAddr.nativeHandleLen(),
        NULL,
        0,
        NULL,
        (LPOVERLAPPED)notifyHandle);
    if (ok) {
        unrecoverableError({
            Error::ErrorCode::CoreBug,
            "Originated from",
            "connectEx",
            "_impl->_clientFD",
            _impl->_clientFD,
        });
    }

    const int myErrno = WSAGetLastError();
    if (myErrno == ERROR_IO_PENDING) {
        return false;
    }

    unrecoverableError({
        Error::ErrorCode::CoreBug,
        "Originated from",
        "connectEx",
        "Errno is",
        myErrno,
        "_impl->_clientFD",
        _impl->_clientFD,
    });
}

bool RawStreamClientHandle::needRetry()
{
    const int iResult = setsockopt(_impl->_clientFD, SOL_SOCKET, SO_UPDATE_CONNECT_CONTEXT, NULL, 0);
    return iResult == -1;
}

void RawStreamClientHandle::destroy()
{
    if (_impl->_clientFD) {
        CancelIoEx((HANDLE)_impl->_clientFD, nullptr);
    }
    if (_impl->_clientFD) {
        closeAndZeroSocket(&_impl->_clientFD);
    }
}

void RawStreamClientHandle::zeroNativeHandle() noexcept
{
    _impl->_clientFD = 0;
}

RawStreamClientHandle::~RawStreamClientHandle()
{
    destroy();
}

}  // namespace ymq
}  // namespace scaler
#endif

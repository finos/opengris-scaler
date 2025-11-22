#ifdef _WIN32
#include "scaler/error/error.h"
#include "scaler/ymq/internal/defs.h"
#include "scaler/ymq/internal/raw_client_tcp_fd.h"

namespace scaler {
namespace ymq {

RawClientTCPFD::RawClientTCPFD(sockaddr remoteAddr): _clientFD {}, _remoteAddr(std::move(remoteAddr))
{
    _connectExFunc = {};

    auto tmp = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    DWORD res;
    GUID guid = WSAID_CONNECTEX;
    WSAIoctl(
        tmp,
        SIO_GET_EXTENSION_FUNCTION_POINTER,
        (void*)&guid,
        sizeof(GUID),
        &_connectExFunc,
        sizeof(_connectExFunc),
        &res,
        0,
        0);
    closesocket(tmp);
    if (!_connectExFunc) {
        unrecoverableError({
            Error::ErrorCode::CoreBug,
            "Originated from",
            "WSAIoctl",
            "Errno is",
            GetErrorCode(),
            "_connectExFunc",
            (void*)_connectExFunc,
        });
    }
}

void RawClientTCPFD::create()
{
    _clientFD = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (_clientFD == -1) {
        unrecoverableError({
            Error::ErrorCode::CoreBug,
            "Originated from",
            "socket(2)",
            "Errno is",
            strerror(GetErrorCode()),
        });
    }
    u_long nonblock = 1;
    ioctlsocket(_clientFD, FIONBIO, &nonblock);
}

bool RawClientTCPFD::prepConnect(void* notifyHandle)
{
    sockaddr_in localAddr      = {};
    localAddr.sin_family       = AF_INET;
    const char ip4[]           = {127, 0, 0, 1};
    *(int*)&localAddr.sin_addr = *(int*)ip4;

    const int bindRes = bind(_clientFD, (struct sockaddr*)&localAddr, sizeof(struct sockaddr_in));
    if (bindRes == -1) {
        unrecoverableError({
            Error::ErrorCode::ConfigurationError,
            "Originated from",
            "bind",
            "Errno is",
            GetErrorCode(),
            "_clientFD",
            _clientFD,
        });
    }

    const bool ok =
        _connectExFunc(_clientFD, &_remoteAddr, sizeof(struct sockaddr), NULL, 0, NULL, (LPOVERLAPPED)notifyHandle);
    if (ok) {
        unrecoverableError({
            Error::ErrorCode::CoreBug,
            "Originated from",
            "connectEx",
            "_clientFD",
            _clientFD,
        });
    }

    const int myErrno = GetErrorCode();
    if (myErrno == ERROR_IO_PENDING) {
        return false;
    }

    unrecoverableError({
        Error::ErrorCode::CoreBug,
        "Originated from",
        "connectEx",
        "Errno is",
        myErrno,
        "_clientFD",
        _clientFD,
    });
}

bool RawClientTCPFD::needRetry()
{
    const int iResult = setsockopt(_clientFD, SOL_SOCKET, SO_UPDATE_CONNECT_CONTEXT, NULL, 0);
    return iResult == -1;
}

void RawClientTCPFD::destroy()
{
    if (_clientFD) {
        CancelIoEx((HANDLE)_clientFD, nullptr);
    }
    if (_clientFD) {
        CloseAndZeroSocket(_clientFD);
    }
}

void RawClientTCPFD::zeroNativeHandle() noexcept
{
    _clientFD = 0;
}

RawClientTCPFD::~RawClientTCPFD()
{
    destroy();
}

}  // namespace ymq
}  // namespace scaler
#endif

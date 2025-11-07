#include "scaler/ymq/internal/raw_client_tcp_fd.h"

#include "scaler/error/error.h"

namespace scaler {
namespace ymq {

RawClientTCPFD::RawClientTCPFD(sockaddr remoteAddr): _clientFD {}, _remoteAddr(std::move(remoteAddr))
{
#ifdef _WIN32
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
#endif  // _WIN32
}

void RawClientTCPFD::create()
{
#ifdef __linux__
    _clientFD = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, IPPROTO_TCP);

    if ((int)_clientFD == -1) {
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
        }
        return;
    }
#endif

#ifdef _WIN32
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
#endif
}

bool RawClientTCPFD::prepConnect(void* notifyHandle)
{
#ifdef __linux__
    const int ret = connect((int)_clientFD, (sockaddr*)&_remoteAddr, sizeof(_remoteAddr));

    if (ret >= 0) [[unlikely]] {
        return true;
    }

    const int myErrno = errno;
    switch (myErrno) {
        case EINPROGRESS: return false;

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
                "_clientFD",
                _clientFD,
            });

        case EINTR:
            unrecoverableError({
                Error::ErrorCode::SignalNotSupported,
                "Originated from",
                "connect(2)",
                "Errno is",
                strerror(myErrno),
                "_clientFD",
                _clientFD,
            });

        case EPERM:
        case EADDRINUSE:
        case EAFNOSUPPORT:
        case EAGAIN:
        case EALREADY:
        case EBADF:
        case EISCONN:
        case ENOTSOCK:
        case ECONNREFUSED:
        default:
            unrecoverableError({
                Error::ErrorCode::CoreBug,
                "Originated from",
                "connect(2)",
                "Errno is",
                strerror(myErrno),
                "_clientFD",
                _clientFD,
            });
    }
#endif

#ifdef _WIN32
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
#endif
}

bool RawClientTCPFD::needRetry()
{
#ifdef __linux__
    int err {};
    socklen_t errLen {sizeof(err)};
    if (getsockopt((int)_clientFD, SOL_SOCKET, SO_ERROR, &err, &errLen) < 0) {
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
                    "_clientFD",
                    _clientFD,
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
                    "_clientFD",
                    _clientFD,
                });
        }
    }
    if (err != 0) {
        if (err == ECONNREFUSED) {
            return true;
        }

        // Since connect(2) error has been checked previously
        unrecoverableError({
            Error::ErrorCode::CoreBug,
            "Originated from",
            "connect(2)",
            "Errno is",
            strerror(errno),
            "_clientFD",
            _clientFD,
        });
    }
    return false;
#endif

#ifdef _WIN32
    const int iResult = setsockopt(_clientFD, SOL_SOCKET, SO_UPDATE_CONNECT_CONTEXT, NULL, 0);
    return iResult == -1;
#endif
}

void RawClientTCPFD::destroy()
{
#ifdef _WIN32
    if (_clientFD) {
        CancelIoEx((HANDLE)_clientFD, nullptr);
    }
#endif
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

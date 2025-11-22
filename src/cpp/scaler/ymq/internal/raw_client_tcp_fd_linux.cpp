#ifdef __linux__
#include "scaler/error/error.h"
#include "scaler/ymq/internal/raw_client_tcp_fd.h"

namespace scaler {
namespace ymq {

RawClientTCPFD::RawClientTCPFD(sockaddr remoteAddr): _clientFD {}, _remoteAddr(std::move(remoteAddr))
{
}

void RawClientTCPFD::create()
{
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
}

bool RawClientTCPFD::prepConnect(void* notifyHandle)
{
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
}

bool RawClientTCPFD::needRetry()
{
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
}

void RawClientTCPFD::destroy()
{
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

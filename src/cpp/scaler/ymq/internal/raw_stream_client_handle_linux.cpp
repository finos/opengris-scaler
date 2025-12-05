#ifdef __linux__
#include <arpa/inet.h>  // inet_pton
#include <errno.h>      // EAGAIN etc.
#include <limits.h>
#include <sys/socket.h>
#include <unistd.h>

#include "scaler/error/error.h"
#include "scaler/ymq/internal/network_utils.h"
#include "scaler/ymq/internal/raw_stream_client_handle.h"

namespace scaler {
namespace ymq {

struct RawStreamClientHandle::Impl {
    int _clientFD;
    SocketAddress _remoteAddr;
};

uint64_t RawStreamClientHandle::nativeHandle()
{
    return _impl->_clientFD;
};

RawStreamClientHandle::RawStreamClientHandle(SocketAddress remoteAddr)
    : _impl(std::make_unique<RawStreamClientHandle::Impl>())
{
    _impl->_clientFD   = {};
    _impl->_remoteAddr = std::move(remoteAddr);
}

void RawStreamClientHandle::create()
{
    _impl->_clientFD = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, IPPROTO_TCP);

    if (_impl->_clientFD == -1) {
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

bool RawStreamClientHandle::prepConnect(void* notifyHandle)
{
    const int ret = connect(_impl->_clientFD, _impl->_remoteAddr.nativeHandle(), _impl->_remoteAddr.nativeHandleLen());

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
                "_impl->_clientFD",
                _impl->_clientFD,
            });

        case EINTR:
            unrecoverableError({
                Error::ErrorCode::SignalNotSupported,
                "Originated from",
                "connect(2)",
                "Errno is",
                strerror(myErrno),
                "_impl->_clientFD",
                _impl->_clientFD,
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
                "_impl->_clientFD",
                _impl->_clientFD,
            });
    }
}

bool RawStreamClientHandle::needRetry()
{
    int err {};
    socklen_t errLen {sizeof(err)};
    if (getsockopt(_impl->_clientFD, SOL_SOCKET, SO_ERROR, &err, &errLen) < 0) {
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
                    "_impl->_clientFD",
                    _impl->_clientFD,
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
                    "_impl->_clientFD",
                    _impl->_clientFD,
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
            "_impl->_clientFD",
            _impl->_clientFD,
        });
    }
    return false;
}

void RawStreamClientHandle::destroy()
{
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

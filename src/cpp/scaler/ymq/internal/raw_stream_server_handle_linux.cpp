#ifdef __linux__

#include <utility>  // std::move

#include "scaler/error/error.h"
#include "scaler/ymq/internal/defs.h"
#include "scaler/ymq/internal/network_utils.h"
#include "scaler/ymq/internal/raw_stream_server_handle.h"

namespace scaler {
namespace ymq {

RawStreamServerHandle::RawStreamServerHandle(sockaddr addr)
{
    _serverFD = {};
    _addr     = std::move(addr);

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
}

bool RawStreamServerHandle::setReuseAddress()
{
    if (::scaler::ymq::setReuseAddress(_serverFD)) {
        return true;
    } else {
        CloseAndZeroSocket(_serverFD);
        return false;
    }
}

void RawStreamServerHandle::bindAndListen()
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

RawStreamServerHandle::~RawStreamServerHandle()
{
    if (_serverFD) {
        CloseAndZeroSocket(_serverFD);
    }
}

void RawStreamServerHandle::prepareAcceptSocket(void* notifyHandle)
{
    (void)notifyHandle;
}

std::vector<std::pair<uint64_t, sockaddr>> RawStreamServerHandle::getNewConns()
{
    std::vector<std::pair<uint64_t, sockaddr>> res;
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
}

void RawStreamServerHandle::destroy()
{
    if (_serverFD) {
        CloseAndZeroSocket(_serverFD);
    }
}

}  // namespace ymq
}  // namespace scaler

#endif

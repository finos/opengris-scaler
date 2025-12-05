#ifdef __linux__

#include <arpa/inet.h>  // inet_pton
#include <errno.h>      // EAGAIN etc.
#include <limits.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/socket.h>  // ::recv
#include <sys/socket.h>
#include <sys/time.h>  // itimerspec
#include <sys/timerfd.h>
#include <unistd.h>

#include <memory>
#include <utility>  // std::move

#include "scaler/error/error.h"
#include "scaler/ymq/internal/network_utils.h"
#include "scaler/ymq/internal/raw_stream_server_handle.h"
#include "scaler/ymq/internal/socket_address.h"

namespace scaler {
namespace ymq {

struct RawStreamServerHandle::Impl {
    int _serverFD;
    SocketAddress _addr;
};

uint64_t RawStreamServerHandle::nativeHandle()
{
    return _impl->_serverFD;
}

RawStreamServerHandle::RawStreamServerHandle(SocketAddress addr): _impl(std::make_unique<RawStreamServerHandle::Impl>())
{
    _impl->_serverFD = {};
    _impl->_addr     = std::move(addr);

    _impl->_serverFD = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, IPPROTO_TCP);
    if (_impl->_serverFD == -1) {
        unrecoverableError({
            Error::ErrorCode::ConfigurationError,
            "Originated from",
            "socket(2)",
            "Errno is",
            strerror(errno),
            "_serverFD",
            _impl->_serverFD,
        });

        return;
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
            strerror(errno),
            "_serverFD",
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
            strerror(errno),
            "_serverFD",
            serverFD,
        });

        return;
    }
}

RawStreamServerHandle::~RawStreamServerHandle()
{
    if (_impl->_serverFD) {
        closeAndZeroSocket(&_impl->_serverFD);
    }
}

void RawStreamServerHandle::prepareAcceptSocket(void* notifyHandle)
{
    (void)notifyHandle;
}

std::vector<std::pair<uint64_t, SocketAddress>> RawStreamServerHandle::getNewConns()
{
    std::vector<std::pair<uint64_t, SocketAddress>> res;
    while (true) {
        sockaddr remoteAddr {};
        socklen_t remoteAddrLen = sizeof(remoteAddr);

        int fd = accept4(_impl->_serverFD, &remoteAddr, &remoteAddrLen, SOCK_NONBLOCK | SOCK_CLOEXEC);
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
                        _impl->_serverFD,
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
    if (_impl->_serverFD) {
        closeAndZeroSocket(&_impl->_serverFD);
    }
}

}  // namespace ymq
}  // namespace scaler

#endif

#include "scaler/ymq/internal/socket_address.h"
#ifdef __linux__

#include <utility>  // std::move

#include "scaler/error/error.h"
#include "scaler/ymq/internal/defs.h"
#include "scaler/ymq/internal/network_utils.h"
#include "scaler/ymq/internal/raw_stream_server_handle.h"

namespace scaler {
namespace ymq {

RawStreamServerHandle::RawStreamServerHandle(SocketAddress address): _address(std::move(address))
{
    _serverFD = {};
    switch (_address._type) {
        case SocketAddress::Type::TCP: _serverFD = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, IPPROTO_TCP); break;
        case SocketAddress::Type::IPC: _serverFD = socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0); break;
        default: std::unreachable();
    }

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
    if (_address._type == SocketAddress::Type::IPC) {
        unlink(_address._addr.sun_path);
    }

    if (bind(_serverFD, (sockaddr*)&_address._addr, _address._addrLen) == -1) {
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

std::vector<std::pair<uint64_t, SocketAddress>> RawStreamServerHandle::getNewConns()
{
    std::vector<std::pair<uint64_t, SocketAddress>> res;
    while (true) {
        sockaddr_un remoteAddr {};
        socklen_t remoteAddrLen = _address._addrLen;

        int fd = accept4(_serverFD, (sockaddr*)&remoteAddr, &remoteAddrLen, SOCK_NONBLOCK | SOCK_CLOEXEC);
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
        SocketAddress socketAddress {};
        socketAddress._addrLen = sizeof(remoteAddr);
        socketAddress._type =
            sizeof(remoteAddr) == sizeof(sockaddr) ? SocketAddress::Type::TCP : SocketAddress::Type::IPC;
        memcpy(&socketAddress._addr, &remoteAddr, socketAddress._addrLen);

        res.push_back({fd, std::move(socketAddress)});
    }
}

void RawStreamServerHandle::destroy()
{
    if (_serverFD) {
        CloseAndZeroSocket(_serverFD);
    }
    if (_address._type == SocketAddress::Type::IPC) {
        unlink(_address._addr.sun_path);
    }
}

}  // namespace ymq
}  // namespace scaler

#endif

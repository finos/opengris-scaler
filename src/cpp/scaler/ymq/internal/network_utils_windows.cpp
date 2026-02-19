#ifdef _WIN32

#include <cstdint>
#include <string>

#include "scaler/error/error.h"
#include "scaler/ymq/internal/network_utils.h"

// clang-format off
#define NOMINMAX
#include <windows.h>
#include <winsock2.h>
#include <mswsock.h>
// clang-format on

#undef SendMessageCallback
#define __PRETTY_FUNCTION__ __FUNCSIG__

namespace scaler {
namespace ymq {

void closeAndZeroSocket(void* fd)
{
    uint64_t* tmp = (uint64_t*)fd;
    closesocket(*tmp);
    *tmp = 0;
}

std::expected<SocketAddress, Error> stringToSockaddr(const std::string& address)
{
    // Check and strip the "tcp://" prefix
    static const std::string prefix = "tcp://";
    if (address.substr(0, prefix.size()) != prefix) {
        return std::unexpected(
            Error {
                Error::ErrorCode::InvalidAddressFormat,
                "Originated from",
                __PRETTY_FUNCTION__,
                "Your input is",
                address,
            });
    }

    const std::string addrPart = address.substr(prefix.size());
    const size_t colonPos      = addrPart.find(':');
    if (colonPos == std::string::npos) {
        return std::unexpected(
            Error {
                Error::ErrorCode::InvalidAddressFormat,
                "Originated from",
                __PRETTY_FUNCTION__,
                "Your input is",
                address,
            });
    }

    const std::string ip      = addrPart.substr(0, colonPos);
    const std::string portStr = addrPart.substr(colonPos + 1);

    int port = 0;
    try {
        port = std::stoi(portStr);
    } catch (...) {
        return std::unexpected(
            Error {
                Error::ErrorCode::InvalidAddressFormat,
                "Originated from",
                __PRETTY_FUNCTION__,
                "Your input is",
                address,
            });
    }

    sockaddr_in outAddr {};
    outAddr.sin_family = AF_INET;
    outAddr.sin_port   = htons(port);

    int res = inet_pton(AF_INET, ip.c_str(), &outAddr.sin_addr);
    if (res == 0) {
        return std::unexpected(
            Error {
                Error::ErrorCode::InvalidAddressFormat,
                "Originated from",
                __PRETTY_FUNCTION__,
                "Your input is",
                address,
            });
    }

    if (res == -1) {
        return std::unexpected(
            Error {
                Error::ErrorCode::ConfigurationError,
                "Originated from",
                __PRETTY_FUNCTION__,
                "Errno is",
                strerror(errno),
                "out_addr.sin_family",
                outAddr.sin_family,
                "out_addr.sin_port",
                outAddr.sin_port,
            });
    }

    return SocketAddress((const sockaddr*)&outAddr);
}

std::expected<SocketAddress, Error> stringToSocketAddress(const std::string& address)
{
    assert(address.size());
    switch (address[0]) {
        case 't': return stringToSockaddr(address);  // TCP
        case 'i':                                    // IPC
            return std::unexpected(
                Error {
                    Error::ErrorCode::IPCOnWinNotSupported,
                    "Originated from",
                    __PRETTY_FUNCTION__,
                    "Your input is",
                    address,
                });
        default:
            return std::unexpected(
                Error {
                    Error::ErrorCode::InvalidAddressFormat,
                    "Originated from",
                    __PRETTY_FUNCTION__,
                    "Your input is",
                    address,
                });
    }
}

int setNoDelay(int fd)
{
    int optval = 1;
    if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (const char*)&optval, sizeof(optval)) == -1) {
        unrecoverableError({
            Error::ErrorCode::ConfigurationError,
            "Originated from",
            __PRETTY_FUNCTION__,
            "Errno is",
            strerror(errno),
            "fd",
            fd,
        });
    }

    return fd;
}

SocketAddress getLocalAddr(int fd)
{
    sockaddr localAddr     = {};
    socklen_t localAddrLen = sizeof(localAddr);
    if (getsockname(fd, &localAddr, &localAddrLen) == -1) {
        unrecoverableError({
            Error::ErrorCode::ConfigurationError,
            "Originated from",
            __PRETTY_FUNCTION__,
            "Errno is",
            strerror(errno),
            "fd",
            fd,
        });
    }
    return SocketAddress((const sockaddr*)&localAddr);
}

SocketAddress getRemoteAddr(int fd)
{
    sockaddr remoteAddr     = {};
    socklen_t remoteAddrLen = sizeof(remoteAddr);

    if (getpeername(fd, &remoteAddr, &remoteAddrLen) == -1) {
        unrecoverableError({
            Error::ErrorCode::ConfigurationError,
            "Originated from",
            __PRETTY_FUNCTION__,
            "Errno is",
            strerror(errno),
            "fd",
            fd,
        });
    }

    return SocketAddress((const sockaddr*)&remoteAddr);
}

}  // namespace ymq
}  // namespace scaler

#endif

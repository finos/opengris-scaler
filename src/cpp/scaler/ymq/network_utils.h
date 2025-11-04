#pragma once

#include <string.h>

#include <cassert>
#include <expected>

#include "scaler/error/error.h"
#include "scaler/ymq/internal/defs.h"

namespace scaler {
namespace ymq {

inline auto GetErrorCode()
{
#ifdef __linux__
    return errno;
#endif  // __linux__
#ifdef _WIN32
    return WSAGetLastError();
#endif  // _WIN32
}

inline constexpr void CloseAndZeroSocket(auto& fd)
{
#ifdef __linux__
    close(fd);
#endif  // __linux__
#ifdef _WIN32
    closesocket(fd);
#endif  // _WIN32
    fd = 0;
}

inline std::expected<sockaddr, int> stringToSockaddr(const std::string& address)
{
    // Check and strip the "tcp://" prefix
    static const std::string prefix = "tcp://";
    if (address.substr(0, prefix.size()) != prefix) {
        unrecoverableError({
            Error::ErrorCode::InvalidAddressFormat,
            "Originated from",
            __PRETTY_FUNCTION__,
            "Your input is",
            address,
        });
    }

    std::string addr_part = address.substr(prefix.size());
    size_t colon_pos      = addr_part.find(':');
    if (colon_pos == std::string::npos) {
        unrecoverableError({
            Error::ErrorCode::InvalidAddressFormat,
            "Originated from",
            __PRETTY_FUNCTION__,
            "Your input is",
            address,
        });
    }

    std::string ip       = addr_part.substr(0, colon_pos);
    std::string port_str = addr_part.substr(colon_pos + 1);

    int port = 0;
    try {
        port = std::stoi(port_str);
    } catch (...) {
        unrecoverableError({
            Error::ErrorCode::InvalidAddressFormat,
            "Originated from",
            __PRETTY_FUNCTION__,
            "Your input is",
            address,
        });
    }

    sockaddr_in out_addr {};
    out_addr.sin_family = AF_INET;
    out_addr.sin_port   = htons(port);

    int res = inet_pton(AF_INET, ip.c_str(), &out_addr.sin_addr);
    if (res == 0) {
        unrecoverableError({
            Error::ErrorCode::InvalidAddressFormat,
            "Originated from",
            __PRETTY_FUNCTION__,
            "Your input is",
            address,
        });
    }

    if (res == -1) {
        unrecoverableError({
            Error::ErrorCode::ConfigurationError,
            "Originated from",
            __PRETTY_FUNCTION__,
            "Errno is",
            strerror(errno),
            "out_addr.sin_family",
            out_addr.sin_family,
            "out_addr.sin_port",
            out_addr.sin_port,
        });
    }

    return *(sockaddr*)&out_addr;
}

inline int setNoDelay(int fd)
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

inline sockaddr getLocalAddr(int fd)
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
    return localAddr;
}

inline sockaddr getRemoteAddr(int fd)
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

    return remoteAddr;
}

}  // namespace ymq
}  // namespace scaler

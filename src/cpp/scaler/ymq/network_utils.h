#pragma once

#include <string.h>

#include <cassert>
#include <expected>

#include "scaler/error/error.h"

namespace scaler {
namespace ymq {

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

    const std::string addrPart = address.substr(prefix.size());
    const size_t colonPos      = addrPart.find(':');
    if (colonPos == std::string::npos) {
        unrecoverableError({
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
        unrecoverableError({
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
            outAddr.sin_family,
            "out_addr.sin_port",
            outAddr.sin_port,
        });
    }

    return *(sockaddr*)&outAddr;
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

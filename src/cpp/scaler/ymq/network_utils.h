#pragma once

#include <string.h>

#include <cassert>
#include <expected>
#include <utility>

#include "scaler/error/error.h"
#include "scaler/ymq/internal/socket_address.h"

namespace scaler {
namespace ymq {

inline std::expected<sockaddr_un, int> stringToSockaddrUn(const std::string& address)
{
    static const std::string prefix = "ipc://";
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

    sockaddr_un addr {};
    addr.sun_family = AF_UNIX;
    if (addrPart.size() > sizeof(addr.sun_path) - 1) {
        unrecoverableError({
            Error::ErrorCode::InvalidAddressFormat,
            "Originated from",
            __PRETTY_FUNCTION__,
            "Your input is",
            address,
            "Failed due to name too long.",
        });
    }

    strncpy(addr.sun_path, addrPart.c_str(), sizeof(addr.sun_path) - 1);
    return addr;
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

inline SocketAddress stringToSocketAddress(const std::string& address)
{
    assert(address.size());
    SocketAddress res {};

    if (address[0] == 'i') {  // ipc
        const auto tmp = stringToSockaddrUn(address);
        assert(tmp);
        res._type    = SocketAddress::Type::IPC;
        res._addr    = std::move(tmp.value());
        res._addrLen = sizeof(sockaddr_un);
    } else if (address[0] == 't') {  // tcp
        const auto tmp = stringToSockaddr(address);
        assert(tmp);
        res._type              = SocketAddress::Type::TCP;
        *(sockaddr*)&res._addr = std::move(tmp.value());
        res._addrLen           = sizeof(sockaddr);
    } else {
        // Unreachable as we currently don't support other types of system socket
        std::unreachable();
    }
    return res;
}

inline int setNoDelay(int fd)
{
    int optval = 1;
    if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (const char*)&optval, sizeof(optval)) == -1 && errno != EOPNOTSUPP) {
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

inline SocketAddress getLocalAddr(int fd, socklen_t localAddrLen)
{
    sockaddr_un localAddr = {};
    if (getsockname(fd, (sockaddr*)&localAddr, &localAddrLen) == -1) {
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

    SocketAddress res {};

    res._type    = localAddrLen == sizeof(sockaddr) ? SocketAddress::Type::TCP : SocketAddress::Type::IPC;
    res._addrLen = localAddrLen;
    memcpy(&res._addr, &localAddr, res._addrLen);

    return res;
}

inline SocketAddress getRemoteAddr(int fd, socklen_t remoteAddrLen)
{
    sockaddr_un remoteAddr = {};

    if (getpeername(fd, (sockaddr*)&remoteAddr, &remoteAddrLen) == -1) {
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

    SocketAddress res {};

    res._type    = remoteAddrLen == sizeof(sockaddr) ? SocketAddress::Type::TCP : SocketAddress::Type::IPC;
    res._addrLen = remoteAddrLen;
    memcpy(&res._addr, &remoteAddr, res._addrLen);

    return res;
}

}  // namespace ymq
}  // namespace scaler

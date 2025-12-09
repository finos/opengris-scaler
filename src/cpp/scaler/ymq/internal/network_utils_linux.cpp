#ifdef __linux__
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>  // TCP_NODELAY
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>  // close

#include <cassert>
#include <expected>
#include <utility>

#include "scaler/error/error.h"
#include "scaler/ymq/internal/network_utils.h"
#include "scaler/ymq/internal/socket_address.h"

namespace scaler {
namespace ymq {

void closeAndZeroSocket(void* fd)
{
    int* tmp = (int*)fd;
    close(*tmp);
    *tmp = 0;
}

SocketAddress stringToSockaddrUn(const std::string& address)
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
    return SocketAddress((const sockaddr*)&addr);
}

SocketAddress stringToSockaddr(const std::string& address)
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

    return SocketAddress((const sockaddr*)&outAddr);
}

SocketAddress stringToSocketAddress(const std::string& address)
{
    assert(address.size());
    switch (address[0]) {
        case 'i': return stringToSockaddrUn(address);  // IPC
        case 't': return stringToSockaddr(address);    // TCP
        default:
            unrecoverableError({
                Error::ErrorCode::InvalidAddressFormat,
                "Originated from",
                __PRETTY_FUNCTION__,
                "Your input is",
                address,
            });
            break;
    }
}

int setNoDelay(int fd)
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

SocketAddress getLocalAddr(int fd)
{
    sockaddr_un localAddr  = {};
    socklen_t localAddrLen = sizeof(sockaddr_un);
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

    return SocketAddress((const sockaddr*)&localAddr);
}

SocketAddress getRemoteAddr(int fd)
{
    sockaddr_un remoteAddr  = {};
    socklen_t remoteAddrLen = sizeof(sockaddr_un);

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

    return SocketAddress((const sockaddr*)&remoteAddr);
}

}  // namespace ymq
}  // namespace scaler

#endif

#pragma once
#include <memory>

struct sockaddr;
struct sockaddr_un;

namespace scaler {
namespace ymq {

struct SocketAddress {
    enum class Type {
        DEFAULT,
        IPC,
        TCP,
    };

    SocketAddress() noexcept;
    SocketAddress(sockaddr) noexcept;
    SocketAddress(sockaddr_un) noexcept;

    SocketAddress(const SocketAddress& other) noexcept;
    SocketAddress& operator=(const SocketAddress& other) noexcept;
    SocketAddress(SocketAddress&& other) noexcept;
    SocketAddress& operator=(SocketAddress&& other) noexcept;

    sockaddr* nativeHandle() noexcept;
    int nativeHandleLen() noexcept;

    friend void swap(SocketAddress& x, SocketAddress& y) noexcept
    {
        using std::swap;
        swap(x._impl, y._impl);
    }

    struct Impl;
    Impl* _impl;

    ~SocketAddress() noexcept;
};

};  // namespace ymq
};  // namespace scaler

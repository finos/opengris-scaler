#pragma once
#include <utility>

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
    explicit SocketAddress(const sockaddr* addr) noexcept;

    SocketAddress(const SocketAddress& other) noexcept;
    SocketAddress& operator=(const SocketAddress& other) noexcept;
    SocketAddress(SocketAddress&& other) noexcept;
    SocketAddress& operator=(SocketAddress&& other) noexcept;

    sockaddr* nativeHandle() noexcept;
    int nativeHandleLen() const noexcept;
    Type nativeHandleType() const noexcept;

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

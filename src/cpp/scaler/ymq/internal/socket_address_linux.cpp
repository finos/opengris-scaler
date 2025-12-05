#ifdef __linux__
#include <sys/socket.h>
#include <sys/un.h>

#include "scaler/ymq/internal/socket_address.h"

namespace scaler {
namespace ymq {

struct SocketAddress::Impl {
    sockaddr_un _addr;
    socklen_t _addrLen;
    Type _type;
};

SocketAddress::SocketAddress(const SocketAddress& other) noexcept: _impl(new Impl)
{
    _impl->_addr    = other._impl->_addr;
    _impl->_addrLen = other._impl->_addrLen;
    _impl->_type    = other._impl->_type;
}

SocketAddress& SocketAddress::operator=(const SocketAddress& other) noexcept
{
    SocketAddress tmp(other);
    swap(*this, tmp);
    return *this;
}

SocketAddress::SocketAddress(SocketAddress&& other) noexcept: _impl(other._impl)
{
    other._impl = nullptr;
}

SocketAddress& SocketAddress::operator=(SocketAddress&& other) noexcept
{
    if (this != &other) {
        delete _impl;
        _impl       = other._impl;
        other._impl = nullptr;
    }
    return *this;
}

SocketAddress::SocketAddress() noexcept: _impl(new Impl)
{
    _impl->_addr    = {};
    _impl->_addrLen = 0;
    _impl->_type    = SocketAddress::Type::DEFAULT;
}

SocketAddress::SocketAddress(const sockaddr* addr) noexcept: _impl(new Impl)
{
    _impl->_addr    = {};
    _impl->_addrLen = {};
    _impl->_type    = SocketAddress::Type::DEFAULT;

    switch (addr->sa_family) {
        case AF_UNIX:
            *(sockaddr_un*)(&_impl->_addr) = *(const sockaddr_un*)addr;
            _impl->_addrLen                = sizeof(sockaddr_un);
            _impl->_type                   = Type::IPC;
            break;
        case AF_INET:
            *(sockaddr*)(&_impl->_addr) = *(const sockaddr*)addr;
            _impl->_addrLen             = sizeof(sockaddr);
            _impl->_type                = Type::TCP;
            break;

        default: std::unreachable(); break;
    }
}

SocketAddress::~SocketAddress() noexcept
{
    delete _impl;
}

sockaddr* SocketAddress::nativeHandle() noexcept
{
    return (sockaddr*)&_impl->_addr;
}

int SocketAddress::nativeHandleLen() const noexcept
{
    return _impl->_addrLen;
}

SocketAddress::Type SocketAddress::nativeHandleType() const noexcept
{
    return _impl->_type;
}

};  // namespace ymq
};  // namespace scaler

#endif

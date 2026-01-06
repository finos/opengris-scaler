#include "scaler/uv/socket_address.h"

#include <cassert>

namespace scaler {
namespace uv {

std::expected<SocketAddress, Error> SocketAddress::IPv4(const std::string& ip, int port) noexcept
{
    sockaddr_in addr;
    int err = uv_ip4_addr(ip.c_str(), port, &addr);

    if (err) {
        return std::unexpected {Error {err}};
    }

    return SocketAddress(addr);
}

std::expected<SocketAddress, Error> SocketAddress::IPv6(const std::string& ip, int port) noexcept
{
    sockaddr_in6 addr;
    int err = uv_ip6_addr(ip.c_str(), port, &addr);

    if (err) {
        return std::unexpected {Error {err}};
    }

    return SocketAddress(addr);
}

SocketAddress SocketAddress::fromSockAddr(const sockaddr* address) noexcept
{
    if (address->sa_family == AF_INET) {
        const sockaddr_in* addr = reinterpret_cast<const sockaddr_in*>(address);
        return SocketAddress(*addr);
    } else {
        assert(address->sa_family == AF_INET6);
        const sockaddr_in6* addr = reinterpret_cast<const sockaddr_in6*>(address);
        return SocketAddress(*addr);
    }
}

const std::variant<sockaddr_in, sockaddr_in6>& SocketAddress::value() const noexcept
{
    return _value;
}

std::expected<std::string, Error> SocketAddress::name() const noexcept
{
    char buffer[INET6_ADDRSTRLEN];
    int err = uv_ip_name(toSockAddr(), buffer, sizeof(buffer));

    if (err) {
        return std::unexpected {Error {err}};
    }

    return std::string(buffer);
}

int SocketAddress::port() const noexcept
{
    if (std::holds_alternative<sockaddr_in>(_value)) {
        return ntohs(std::get<sockaddr_in>(_value).sin_port);
    } else {
        return ntohs(std::get<sockaddr_in6>(_value).sin6_port);
    }
}

std::expected<std::string, Error> SocketAddress::toString() const noexcept
{
    return name().transform([this](const std::string& name) { return name + ":" + std::to_string(port()); });
}

const sockaddr* SocketAddress::toSockAddr() const noexcept
{
    return reinterpret_cast<const sockaddr*>(&_value);
}

SocketAddress::SocketAddress(std::variant<sockaddr_in, sockaddr_in6>&& value) noexcept: _value(std::move(value))
{
}

}  // namespace uv
}  // namespace scaler

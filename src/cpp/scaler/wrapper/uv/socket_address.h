#pragma once

#include <uv.h>

#include <expected>
#include <string>
#include <variant>

#include "scaler/wrapper/uv/error.h"

namespace scaler {
namespace wrapper {
namespace uv {

// See sockaddr_in, sockaddr_in6
class SocketAddress {
public:
    SocketAddress(std::variant<sockaddr_in, sockaddr_in6> value) noexcept;

    // See uv_ip4_addr
    static std::expected<SocketAddress, Error> IPv4(const std::string& ip, int port) noexcept;

    // See uv_ip6_addr
    static std::expected<SocketAddress, Error> IPv6(const std::string& ip, int port) noexcept;

    static SocketAddress fromSockAddr(const sockaddr* address) noexcept;

    const std::variant<sockaddr_in, sockaddr_in6>& value() const noexcept;

    // See uv_ip_name
    std::expected<std::string, Error> name() const noexcept;

    int port() const noexcept;

    // Returns the string representation of the address, e.g. <name>:<port>.
    std::expected<std::string, Error> toString() const noexcept;

    const sockaddr* toSockAddr() const noexcept;

private:
    std::variant<sockaddr_in, sockaddr_in6> _value;
};

}  // namespace uv
}  // namespace wrapper
}  // namespace scaler

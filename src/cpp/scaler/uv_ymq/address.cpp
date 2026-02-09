#include "scaler/uv_ymq/address.h"

#include <cassert>
#include <utility>

namespace scaler {
namespace uv_ymq {

namespace details {

std::expected<Address, scaler::ymq::Error> fromTCPString(const std::string& addrPart) noexcept
{
    const size_t colonPos = addrPart.rfind(':');
    if (colonPos == std::string::npos) {
        return std::unexpected {
            scaler::ymq::Error {scaler::ymq::Error::ErrorCode::InvalidAddressFormat, "Missing port separator"}};
    }

    const std::string ip      = addrPart.substr(0, colonPos);
    const std::string portStr = addrPart.substr(colonPos + 1);

    int port = 0;
    try {
        port = std::stoi(portStr);
    } catch (...) {
        return std::unexpected {
            scaler::ymq::Error {scaler::ymq::Error::ErrorCode::InvalidPortFormat, "Invalid port number"}};
    }

    // Try IPv4 first
    auto socketAddress = scaler::wrapper::uv::SocketAddress::IPv4(ip, port);
    if (socketAddress.has_value()) {
        return Address(std::move(*socketAddress));
    }

    // Try IPv6
    socketAddress = scaler::wrapper::uv::SocketAddress::IPv6(ip, port);
    if (socketAddress.has_value()) {
        return Address(std::move(*socketAddress));
    }

    return std::unexpected {
        scaler::ymq::Error {scaler::ymq::Error::ErrorCode::InvalidAddressFormat, "Failed to parse IP address"}};
}

}  // namespace details

Address::Address(std::variant<scaler::wrapper::uv::SocketAddress, std::string> value) noexcept: _value(std::move(value))
{
}

const std::variant<scaler::wrapper::uv::SocketAddress, std::string>& Address::value() const noexcept
{
    return _value;
}

Address::Type Address::type() const noexcept
{
    if (std::holds_alternative<scaler::wrapper::uv::SocketAddress>(_value)) {
        return Type::TCP;
    } else if (std::holds_alternative<std::string>(_value)) {
        return Type::IPC;
    } else {
        std::unreachable();
    }
}

const scaler::wrapper::uv::SocketAddress& Address::asTCP() const noexcept
{
    assert(type() == Type::TCP);
    return std::get<scaler::wrapper::uv::SocketAddress>(_value);
}

const std::string& Address::asIPC() const noexcept
{
    assert(type() == Type::TCP);
    return std::get<std::string>(_value);
}

std::expected<Address, scaler::ymq::Error> Address::fromString(const std::string& address) noexcept
{
    static constexpr std::string_view tcpPrefix = "tcp://";
    static constexpr std::string_view ipcPrefix = "ipc://";

    if (address.starts_with(tcpPrefix)) {
        return details::fromTCPString(address.substr(tcpPrefix.size()));
    }

    if (address.starts_with(ipcPrefix)) {
        return Address(address.substr(ipcPrefix.size()));
    }

    return std::unexpected {scaler::ymq::Error {
        scaler::ymq::Error::ErrorCode::InvalidAddressFormat, "Address must start with 'tcp://' or 'ipc://'"}};
}

}  // namespace uv_ymq
}  // namespace scaler

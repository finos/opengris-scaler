#include "scaler/ymq/address.h"

#include <cassert>
#include <charconv>
#include <utility>

namespace scaler {
namespace ymq {

namespace details {

std::expected<Address, Error> fromTCPString(std::string_view addrPart) noexcept
{
    const size_t colonPos = addrPart.rfind(':');
    if (colonPos == std::string_view::npos) {
        return std::unexpected {Error {Error::ErrorCode::InvalidAddressFormat, "Missing port separator"}};
    }

    const std::string ip      = std::string {addrPart.substr(0, colonPos)};
    const std::string portStr = std::string {addrPart.substr(colonPos + 1)};

    int port = 0;
    try {
        port = std::stoi(portStr);
    } catch (...) {
        return std::unexpected {Error {Error::ErrorCode::InvalidPortFormat, "Invalid port number"}};
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

    return std::unexpected {Error {Error::ErrorCode::InvalidAddressFormat, "Failed to parse IP address"}};
}

// Parse ws://host:port/path or wss://host:port/path.
std::expected<Address, Error> fromWSString(std::string_view addrPart, bool secure) noexcept
{
    // Split authority (host:port) from path
    const size_t slashPos            = addrPart.find('/');
    const std::string_view authority = addrPart.substr(0, slashPos);
    const std::string path = slashPos == std::string_view::npos ? "/" : std::string(addrPart.substr(slashPos));

    const size_t colonPos = authority.rfind(':');
    if (colonPos == std::string_view::npos) {
        return std::unexpected {Error {Error::ErrorCode::InvalidAddressFormat, "Missing port in WebSocket address"}};
    }

    const std::string host    = std::string {authority.substr(0, colonPos)};
    const std::string portStr = std::string {authority.substr(colonPos + 1)};

    int port = 0;
    try {
        port = std::stoi(portStr);
    } catch (...) {
        return std::unexpected {Error {Error::ErrorCode::InvalidPortFormat, "Invalid port in WebSocket address"}};
    }

    if (port < 0 || port > 65535) {
        return std::unexpected {Error {Error::ErrorCode::InvalidPortFormat, "WebSocket port out of range"}};
    }

    auto socketAddress = scaler::wrapper::uv::SocketAddress::IPv4(host, port);
    if (!socketAddress.has_value()) {
        socketAddress = scaler::wrapper::uv::SocketAddress::IPv6(host, port);
    }
    if (!socketAddress.has_value()) {
        return std::unexpected {
            Error {Error::ErrorCode::InvalidAddressFormat, "Failed to parse WebSocket host as IP address"}};
    }

    WebSocketAddress wsAddr {
        .tcpAddress = std::move(*socketAddress),
        .host       = host,
        .port       = static_cast<uint16_t>(port),
        .path       = path,
        .secure     = secure,
    };
    return Address(std::move(wsAddr));
}

}  // namespace details

Address::Address(std::variant<scaler::wrapper::uv::SocketAddress, std::string, WebSocketAddress> value) noexcept
    : _value(std::move(value))
{
}

const std::variant<scaler::wrapper::uv::SocketAddress, std::string, WebSocketAddress>& Address::value() const noexcept
{
    return _value;
}

Address::Type Address::type() const noexcept
{
    if (std::holds_alternative<scaler::wrapper::uv::SocketAddress>(_value)) {
        return Type::TCP;
    } else if (std::holds_alternative<std::string>(_value)) {
        return Type::IPC;
    } else if (std::holds_alternative<WebSocketAddress>(_value)) {
        return Type::WebSocket;
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
    assert(type() == Type::IPC);
    return std::get<std::string>(_value);
}

const WebSocketAddress& Address::asWebSocket() const noexcept
{
    assert(type() == Type::WebSocket);
    return std::get<WebSocketAddress>(_value);
}

std::expected<std::string, Error> Address::toString() const noexcept
{
    switch (type()) {
        case Type::TCP: {
            auto tcpAddrStr = asTCP().toString();
            if (!tcpAddrStr.has_value()) {
                return std::unexpected {
                    Error {Error::ErrorCode::InvalidAddressFormat, "Failed to convert TCP address to string"}};
            }
            return std::string(_tcpPrefix) + tcpAddrStr.value();
        }
        case Type::IPC: return std::string {_ipcPrefix} + asIPC();
        case Type::WebSocket: {
            const auto& ws                = asWebSocket();
            const std::string_view prefix = ws.secure ? _wssPrefix : _wsPrefix;
            return std::string(prefix) + ws.host + ":" + std::to_string(ws.port) + ws.path;
        }
        default: std::unreachable();
    };
}

std::expected<Address, Error> Address::fromString(std::string_view address) noexcept
{
    if (address.starts_with(_tcpPrefix)) {
        return details::fromTCPString(address.substr(_tcpPrefix.size()));
    }

    if (address.starts_with(_ipcPrefix)) {
        return Address(std::string {address.substr(_ipcPrefix.size())});
    }

    if (address.starts_with(_wsPrefix)) {
        return details::fromWSString(address.substr(_wsPrefix.size()), false);
    }

    if (address.starts_with(_wssPrefix)) {
        return details::fromWSString(address.substr(_wssPrefix.size()), true);
    }

    return std::unexpected {Error {
        Error::ErrorCode::InvalidAddressFormat, "Address must start with 'tcp://', 'ipc://', 'ws://', or 'wss://'"}};
}

}  // namespace ymq
}  // namespace scaler

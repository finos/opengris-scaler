#include "scaler/ymq/address.h"

#include <array>
#include <cassert>
#include <charconv>
#include <utility>

#include "scaler/wrapper/uv/error.h"

namespace scaler {
namespace ymq {

namespace details {

// Parse a "ip:port" address.
std::expected<Address::AddressValue, Error> fromTCPString(std::string_view addrPart) noexcept
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
        return *socketAddress;
    }

    // Try IPv6
    socketAddress = scaler::wrapper::uv::SocketAddress::IPv6(ip, port);
    if (socketAddress.has_value()) {
        return *socketAddress;
    }

    return std::unexpected {Error {Error::ErrorCode::InvalidAddressFormat, "Failed to parse IP address"}};
}

std::expected<Address::AddressValue, Error> fromIPCString(std::string_view addrPart) noexcept
{
    return {std::string {addrPart}};
}

// Parse host:port/path address.
std::expected<Address::AddressValue, Error> fromWSString(std::string_view addrPart) noexcept
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

    return WebSocketAddress {
        .tcpAddress = std::move(*socketAddress),
        .host       = host,
        .port       = static_cast<uint16_t>(port),
        .path       = path,
    };
}

}  // namespace details

Address::Address(AddressValue value, bool secure, std::optional<TLSConfig> tlsConfig) noexcept
    : _value(std::move(value)), _secure(secure), _tlsConfig(std::move(tlsConfig))
{
}

const Address::AddressValue& Address::value() const noexcept
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

bool Address::secure() const noexcept
{
    return _secure;
}

const std::optional<TLSConfig>& Address::tlsConfig() const noexcept
{
    return _tlsConfig;
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
            const std::string_view prefix = _secure ? _tlsPrefix : _tcpPrefix;
            return std::string(prefix) + tcpAddrStr.value();
        }
        case Type::IPC: return std::string {_ipcPrefix} + asIPC();
        case Type::WebSocket: {
            const auto& ws                = asWebSocket();
            const std::string_view prefix = _secure ? _wssPrefix : _wsPrefix;
            return std::string(prefix) + ws.host + ":" + std::to_string(ws.port) + ws.path;
        }
        default: std::unreachable();
    };
}

std::expected<Address, Error> Address::fromString(std::string_view address, std::optional<TLSConfig> tlsConfig) noexcept
{
    using ParseFunction = std::expected<AddressValue, Error> (*)(std::string_view);

    struct PrefixEntry {
        std::string_view prefix;
        ParseFunction parser;
        bool secure;
    };

    static constexpr std::array<PrefixEntry, 5> prefixParsers {{
        {_tcpPrefix, details::fromTCPString, false},
        {_tlsPrefix, details::fromTCPString, true},
        {_ipcPrefix, details::fromIPCString, false},
        {_wsPrefix, details::fromWSString, false},
        {_wssPrefix, details::fromWSString, true},
    }};

    for (const PrefixEntry& entry: prefixParsers) {
        if (address.starts_with(entry.prefix)) {
            auto result = entry.parser(address.substr(entry.prefix.size()));
            if (!result.has_value()) {
                return std::unexpected {result.error()};
            }
            return Address(std::move(*result), entry.secure, std::move(tlsConfig));
        }
    }

    return std::unexpected {Error {
        Error::ErrorCode::InvalidAddressFormat,
        "Address must start with 'tcp://', 'tls://', 'ipc://', 'ws://', or 'wss://'"}};
}

std::expected<std::optional<scaler::wrapper::openssl::SSLContext>, Error> Address::getSSLContext() const noexcept
{
    if (!_secure) {
        return std::nullopt;
    }

    if (_tlsConfig.has_value()) {
        auto context = _tlsConfig->getSSLContext();
        if (!context.has_value()) {
            return std::unexpected {std::move(context.error())};
        }
        return std::move(context.value());
    }

    // No TLS config provided, use default SSL context.
    auto context = scaler::wrapper::openssl::SSLContext::init();
    if (!context.has_value()) {
        return std::unexpected {Error {Error::ErrorCode::SysCallError, "SSLContext::init() failed"}};
    }
    return std::move(context.value());
}

}  // namespace ymq
}  // namespace scaler

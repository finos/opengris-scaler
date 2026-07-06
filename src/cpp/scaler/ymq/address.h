#pragma once

#include <cstdint>
#include <expected>
#include <optional>
#include <string>
#include <string_view>
#include <variant>

#include "scaler/error/error.h"
#include "scaler/wrapper/openssl/ssl_context.h"
#include "scaler/wrapper/uv/socket_address.h"
#include "scaler/ymq/tls_config.h"

namespace scaler {
namespace ymq {

// Parsed representation of a ws:// or wss:// address.
struct WebSocketAddress {
    scaler::wrapper::uv::SocketAddress tcpAddress;  // resolved TCP address for the underlying connection
    std::string host;                               // original hostname (for the HTTP Host header)
    uint16_t port;                                  // port number
    std::string path;                               // request path, always starts with '/'
};

// A socket address, can either be a SocketAddress (IPv4/6), an IPC path, or a WebSocket address.
class Address {
public:
    using AddressValue = std::variant<scaler::wrapper::uv::SocketAddress, std::string, WebSocketAddress>;

    enum class Type {
        IPC,
        TCP,
        WebSocket,
    };

    Address(AddressValue value, bool secure = false, std::optional<TLSConfig> tlsConfig = std::nullopt) noexcept;

    Address(const Address&) noexcept            = default;
    Address& operator=(const Address&) noexcept = default;

    Address(Address&&) noexcept            = default;
    Address& operator=(Address&&) noexcept = default;

    const AddressValue& value() const noexcept;

    Type type() const noexcept;

    // Whether this address requires TLS/SSL.
    bool secure() const noexcept;

    const std::optional<TLSConfig>& tlsConfig() const noexcept;

    const scaler::wrapper::uv::SocketAddress& asTCP() const noexcept;

    const std::string& asIPC() const noexcept;

    const WebSocketAddress& asWebSocket() const noexcept;

    std::expected<std::string, Error> toString() const noexcept;

    // Try to parse a string to an Address instance.
    //
    // Example of string values are:
    //
    //     ipc://some_ipc_socket_name
    //     tcp://127.0.0.1:1827
    //     tls://127.0.0.1:1827
    //     tcp://2001:db8::1:1211
    //     ws://127.0.0.1:8765/
    //     wss://example.com:443/ymq
    //
    static std::expected<Address, Error> fromString(
        std::string_view address, std::optional<TLSConfig> tlsConfig = std::nullopt) noexcept;

    // Build an SSLContext from the address's encryption requirement.
    //
    // Returns std::nullopt if the address is not secure.
    std::expected<std::optional<scaler::wrapper::openssl::SSLContext>, Error> getSSLContext() const noexcept;

private:
    static constexpr std::string_view _tcpPrefix = "tcp://";
    static constexpr std::string_view _tlsPrefix = "tls://";
    static constexpr std::string_view _ipcPrefix = "ipc://";
    static constexpr std::string_view _wsPrefix  = "ws://";
    static constexpr std::string_view _wssPrefix = "wss://";

    AddressValue _value;

    bool _secure;

    std::optional<TLSConfig> _tlsConfig;
};

}  // namespace ymq
}  // namespace scaler

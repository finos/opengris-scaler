#pragma once

#include <expected>
#include <string>
#include <variant>

#include "scaler/error/error.h"
#include "scaler/wrapper/uv/socket_address.h"

namespace scaler {
namespace uv_ymq {

// A socket address, can either be a SocketAddress (IPv4/6) or an IPC path.
class Address {
public:
    enum class Type {
        IPC,
        TCP,
    };

    Address(std::variant<scaler::wrapper::uv::SocketAddress, std::string> value) noexcept;

    Address(const Address&) noexcept            = default;
    Address& operator=(const Address&) noexcept = default;

    Address(Address&&) noexcept            = default;
    Address& operator=(Address&&) noexcept = default;

    const std::variant<scaler::wrapper::uv::SocketAddress, std::string>& value() const noexcept;

    Type type() const noexcept;

    const scaler::wrapper::uv::SocketAddress& asTCP() const noexcept;

    const std::string& asIPC() const noexcept;

    std::expected<std::string, scaler::ymq::Error> toString() const noexcept;

    // Try to parse a string to an Address instance.
    //
    // Example of string values are:
    //
    //     ipc://some_ipc_socket_name
    //     tcp://127.0.0.1:1827
    //     tcp://2001:db8::1:1211
    //
    static std::expected<Address, scaler::ymq::Error> fromString(const std::string& address) noexcept;

private:
    static constexpr std::string_view _tcpPrefix = "tcp://";
    static constexpr std::string_view _ipcPrefix = "ipc://";

    std::variant<scaler::wrapper::uv::SocketAddress, std::string> _value;
};

}  // namespace uv_ymq
}  // namespace scaler

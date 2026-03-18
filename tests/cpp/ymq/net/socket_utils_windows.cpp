#include <format>
#include <memory>

#include "scaler/ymq/address.h"
#include "tests/cpp/ymq/net/socket_utils.h"
#include "tests/cpp/ymq/net/tcp_socket.h"

std::unique_ptr<Socket> connectSocket(std::string& address_str)
{
    auto address = scaler::ymq::Address::fromString(address_str).value();

    if (address.type() == scaler::ymq::Address::Type::TCP) {
        auto socket = std::make_unique<TCPSocket>();
        socket->tryConnect(address);
        return socket;
    }

    throw std::runtime_error(std::format("Unsupported protocol for raw client: '{}'", address_str));
}

std::unique_ptr<Socket> bindSocket(std::string& address_str)
{
    auto address = scaler::ymq::Address::fromString(address_str).value();

    if (address.type() == scaler::ymq::Address::Type::TCP) {
        auto socket = std::make_unique<TCPSocket>();
        socket->bind(address);
        return socket;
    }

    throw std::runtime_error(std::format("Unsupported protocol for raw server: '{}'", address_str));
}

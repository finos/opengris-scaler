#include <format>
#include <memory>

#include "tests/cpp/ymq/net/address.h"
#include "tests/cpp/ymq/net/socket_utils.h"
#include "tests/cpp/ymq/net/tcp_socket.h"

std::unique_ptr<ISocket> connect_socket(std::string& address_str)
{
    auto address = parseAddress(address_str);

    if (address.protocol == "tcp") {
        auto socket = std::make_unique<TCPSocket>();
        socket->try_connect(address_str);
        return socket;
    }

    throw std::runtime_error(std::format("Unsupported protocol for raw client: '{}'", address.protocol));
}

std::unique_ptr<ISocket> bind_socket(std::string& address_str)
{
    auto address = parseAddress(address_str);

    if (address.protocol == "tcp") {
        auto socket = std::make_unique<TCPSocket>();
        socket->bind(address_str);
        return socket;
    }
    throw std::runtime_error(std::format("Unsupported protocol for raw server: '{}'", address.protocol));
}

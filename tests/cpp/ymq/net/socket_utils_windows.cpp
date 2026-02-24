#include <format>
#include <memory>

#include "scaler/ymq/internal/network_utils.h"
#include "scaler/ymq/internal/socket_address.h"
#include "tests/cpp/ymq/net/socket_utils.h"
#include "tests/cpp/ymq/net/tcp_socket.h"

using scaler::ymq::SocketAddress;

std::unique_ptr<Socket> connectSocket(std::string& address_str)
{
    auto address = scaler::ymq::stringToSocketAddress(address_str).value();

    if (address.nativeHandleType() == SocketAddress::Type::TCP) {
        auto socket = std::make_unique<TCPSocket>();
        socket->tryConnect(address_str);
        return socket;
    }

    throw std::runtime_error(std::format("Unsupported protocol for raw client: '{}'", address.nativeHandleType()));
}

std::unique_ptr<Socket> bindSocket(std::string& address_str)
{
    auto address = scaler::ymq::stringToSocketAddress(address_str).value();

    if (address.nativeHandleType() == SocketAddress::Type::TCP) {
        auto socket = std::make_unique<TCPSocket>();
        socket->bind(address_str);
        return socket;
    }
    throw std::runtime_error(std::format("Unsupported protocol for raw server: '{}'", address.nativeHandleType()));
}

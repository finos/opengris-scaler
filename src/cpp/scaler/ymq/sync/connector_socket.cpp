#include "scaler/ymq/sync/connector_socket.h"

#include <utility>

namespace scaler {
namespace ymq {
namespace sync {

std::expected<ConnectorSocket, Error> ConnectorSocket::connect(
    IOContext& context,
    Identity identity,
    std::string address,
    size_t maxRetryTimes,
    std::chrono::milliseconds initRetryDelay) noexcept
{
    auto result = future::ConnectorSocket::connect(
        context, std::move(identity), std::move(address), maxRetryTimes, initRetryDelay);
    if (!result.has_value()) {
        return std::unexpected(result.error());
    }
    return ConnectorSocket(std::move(result.value()));
}

std::expected<std::pair<ConnectorSocket, Address>, Error> ConnectorSocket::bind(
    IOContext& context, Identity identity, std::string address) noexcept
{
    auto result = future::ConnectorSocket::bind(context, std::move(identity), std::move(address));
    if (!result.has_value()) {
        return std::unexpected(result.error());
    }
    auto [socket, boundAddress] = std::move(result.value());
    return std::make_pair(ConnectorSocket(std::move(socket)), std::move(boundAddress));
}

ConnectorSocket::ConnectorSocket(future::ConnectorSocket socket) noexcept: _socket(std::move(socket))
{
}

const Identity& ConnectorSocket::identity() const noexcept
{
    return _socket.identity();
}

std::expected<void, Error> ConnectorSocket::sendMessage(Bytes messagePayload) noexcept
{
    return _socket.sendMessage(std::move(messagePayload)).get();
}

std::expected<Message, Error> ConnectorSocket::recvMessage() noexcept
{
    return _socket.recvMessage().get();
}

}  // namespace sync
}  // namespace ymq
}  // namespace scaler

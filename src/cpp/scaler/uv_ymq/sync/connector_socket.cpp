#include "scaler/uv_ymq/sync/connector_socket.h"

#include <utility>

namespace scaler {
namespace uv_ymq {
namespace sync {

std::expected<ConnectorSocket, scaler::ymq::Error> ConnectorSocket::init(
    IOContext& context,
    Identity identity,
    std::string address,
    size_t maxRetryTimes,
    std::chrono::milliseconds initRetryDelay)
{
    auto result =
        future::ConnectorSocket::init(context, std::move(identity), std::move(address), maxRetryTimes, initRetryDelay);
    if (!result.has_value()) {
        return std::unexpected(result.error());
    }
    return ConnectorSocket(std::move(result.value()));
}

ConnectorSocket::ConnectorSocket(future::ConnectorSocket socket) noexcept: _socket(std::move(socket))
{
}

const Identity& ConnectorSocket::identity() const noexcept
{
    return _socket.identity();
}

std::expected<void, scaler::ymq::Error> ConnectorSocket::sendMessage(scaler::ymq::Bytes messagePayload)
{
    return _socket.sendMessage(std::move(messagePayload)).get();
}

std::expected<scaler::ymq::Message, scaler::ymq::Error> ConnectorSocket::recvMessage()
{
    return _socket.recvMessage().get();
}

}  // namespace sync
}  // namespace uv_ymq
}  // namespace scaler

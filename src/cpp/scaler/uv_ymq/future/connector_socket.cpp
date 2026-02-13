#include "scaler/uv_ymq/future/connector_socket.h"

#include <memory>
#include <utility>

namespace scaler {
namespace uv_ymq {
namespace future {

std::expected<ConnectorSocket, scaler::ymq::Error> ConnectorSocket::init(
    IOContext& context,
    Identity identity,
    std::string address,
    size_t maxRetryTimes,
    std::chrono::milliseconds initRetryDelay)
{
    std::promise<std::expected<void, scaler::ymq::Error>> promise {};
    auto future = promise.get_future();

    auto socket = scaler::uv_ymq::ConnectorSocket(
        context,
        std::move(identity),
        std::move(address),
        [promise = std::move(promise)](std::expected<void, scaler::ymq::Error> result) mutable {
            promise.set_value(std::move(result));
        },
        maxRetryTimes,
        initRetryDelay);

    auto connectResult = future.get();
    if (!connectResult.has_value()) {
        return std::unexpected(connectResult.error());
    }

    return ConnectorSocket(std::move(socket));
}

ConnectorSocket::ConnectorSocket(scaler::uv_ymq::ConnectorSocket socket) noexcept: _socket(std::move(socket))
{
}

const Identity& ConnectorSocket::identity() const noexcept
{
    return _socket.identity();
}

std::future<std::expected<void, scaler::ymq::Error>> ConnectorSocket::sendMessage(scaler::ymq::Bytes messagePayload)
{
    std::promise<std::expected<void, scaler::ymq::Error>> promise {};
    auto future = promise.get_future();

    _socket.sendMessage(
        std::move(messagePayload),
        [promise = std::move(promise)](std::expected<void, scaler::ymq::Error> result) mutable {
            promise.set_value(std::move(result));
        });

    return future;
}

std::future<std::expected<scaler::ymq::Message, scaler::ymq::Error>> ConnectorSocket::recvMessage()
{
    std::promise<std::expected<scaler::ymq::Message, scaler::ymq::Error>> promise {};
    auto future = promise.get_future();

    _socket.recvMessage(
        [promise = std::move(promise)](std::expected<scaler::ymq::Message, scaler::ymq::Error> result) mutable {
            promise.set_value(std::move(result));
        });

    return future;
}

}  // namespace future
}  // namespace uv_ymq
}  // namespace scaler

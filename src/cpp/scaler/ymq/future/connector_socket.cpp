#include "scaler/ymq/future/connector_socket.h"

#include <memory>
#include <utility>

namespace scaler {
namespace ymq {
namespace future {

std::expected<ConnectorSocket, scaler::ymq::Error> ConnectorSocket::connect(
    IOContext& context,
    Identity identity,
    std::string address,
    size_t maxRetryTimes,
    std::chrono::milliseconds initRetryDelay)
{
    std::promise<std::expected<void, scaler::ymq::Error>> promise {};
    auto future = promise.get_future();

    auto socket = scaler::ymq::ConnectorSocket::connect(
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

std::expected<std::pair<ConnectorSocket, Address>, scaler::ymq::Error> ConnectorSocket::bind(
    IOContext& context, Identity identity, std::string address)
{
    std::promise<std::expected<Address, scaler::ymq::Error>> promise {};
    auto future = promise.get_future();

    auto socket = scaler::ymq::ConnectorSocket::bind(
        context,
        std::move(identity),
        std::move(address),
        [promise = std::move(promise)](std::expected<Address, scaler::ymq::Error> result) mutable {
            promise.set_value(std::move(result));
        });

    auto bindResult = future.get();
    if (!bindResult.has_value()) {
        return std::unexpected(bindResult.error());
    }

    return std::make_pair(ConnectorSocket(std::move(socket)), std::move(bindResult.value()));
}

ConnectorSocket::ConnectorSocket(scaler::ymq::ConnectorSocket socket) noexcept: _socket(std::move(socket))
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
}  // namespace ymq
}  // namespace scaler

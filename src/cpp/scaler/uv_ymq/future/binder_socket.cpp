#include "scaler/uv_ymq/future/binder_socket.h"

#include <memory>
#include <utility>

namespace scaler {
namespace uv_ymq {
namespace future {

BinderSocket::BinderSocket(IOContext& context, Identity identity) noexcept: _socket(context, std::move(identity))
{
}

const Identity& BinderSocket::identity() const noexcept
{
    return _socket.identity();
}

std::future<std::expected<Address, scaler::ymq::Error>> BinderSocket::bindTo(std::string address)
{
    std::promise<std::expected<Address, scaler::ymq::Error>> promise {};
    auto future = promise.get_future();

    _socket.bindTo(
        std::move(address), [promise = std::move(promise)](std::expected<Address, scaler::ymq::Error> result) mutable {
            promise.set_value(std::move(result));
        });

    return future;
}

std::future<std::expected<void, scaler::ymq::Error>> BinderSocket::sendMessage(
    Identity remoteIdentity, scaler::ymq::Bytes messagePayload)
{
    std::promise<std::expected<void, scaler::ymq::Error>> promise {};
    auto future = promise.get_future();

    _socket.sendMessage(
        std::move(remoteIdentity),
        std::move(messagePayload),
        [promise = std::move(promise)](std::expected<void, scaler::ymq::Error> result) mutable {
            promise.set_value(std::move(result));
        });

    return future;
}

std::future<std::expected<scaler::ymq::Message, scaler::ymq::Error>> BinderSocket::recvMessage()
{
    std::promise<std::expected<scaler::ymq::Message, scaler::ymq::Error>> promise {};
    auto future = promise.get_future();

    _socket.recvMessage(
        [promise = std::move(promise)](std::expected<scaler::ymq::Message, scaler::ymq::Error> result) mutable {
            promise.set_value(std::move(result));
        });

    return future;
}

void BinderSocket::closeConnection(Identity remoteIdentity) noexcept
{
    _socket.closeConnection(std::move(remoteIdentity));
}

}  // namespace future
}  // namespace uv_ymq
}  // namespace scaler

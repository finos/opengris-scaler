#include "scaler/ymq/future/binder_socket.h"

#include <memory>
#include <utility>

namespace scaler {
namespace ymq {
namespace future {

BinderSocket::BinderSocket(IOContext& context, Identity identity) noexcept: _socket(context, std::move(identity))
{
}

const Identity& BinderSocket::identity() const noexcept
{
    return _socket.identity();
}

std::future<std::expected<Address, Error>> BinderSocket::bindTo(std::string address)
{
    std::promise<std::expected<Address, Error>> promise {};
    auto future = promise.get_future();

    _socket.bindTo(std::move(address), [promise = std::move(promise)](std::expected<Address, Error> result) mutable {
        promise.set_value(std::move(result));
    });

    return future;
}

std::future<std::expected<void, Error>> BinderSocket::sendMessage(Identity remoteIdentity, Bytes messagePayload)
{
    std::promise<std::expected<void, Error>> promise {};
    auto future = promise.get_future();

    _socket.sendMessage(
        std::move(remoteIdentity),
        std::move(messagePayload),
        [promise = std::move(promise)](std::expected<void, Error> result) mutable {
            promise.set_value(std::move(result));
        });

    return future;
}

void BinderSocket::sendMulticastMessage(Bytes messagePayload, std::optional<Identity> remotePrefix) noexcept
{
    _socket.sendMulticastMessage(std::move(messagePayload), std::move(remotePrefix));
}

std::future<std::expected<Message, Error>> BinderSocket::recvMessage()
{
    std::promise<std::expected<Message, Error>> promise {};
    auto future = promise.get_future();

    _socket.recvMessage([promise = std::move(promise)](std::expected<Message, Error> result) mutable {
        promise.set_value(std::move(result));
    });

    return future;
}

void BinderSocket::closeConnection(Identity remoteIdentity) noexcept
{
    _socket.closeConnection(std::move(remoteIdentity));
}

}  // namespace future
}  // namespace ymq
}  // namespace scaler

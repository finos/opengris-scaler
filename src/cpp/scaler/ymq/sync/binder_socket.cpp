#include "scaler/ymq/sync/binder_socket.h"

#include <utility>

namespace scaler {
namespace ymq {
namespace sync {

BinderSocket::BinderSocket(IOContext& context, Identity identity) noexcept: _socket(context, std::move(identity))
{
}

const Identity& BinderSocket::identity() const noexcept
{
    return _socket.identity();
}

std::expected<Address, Error> BinderSocket::bindTo(std::string address)
{
    return _socket.bindTo(std::move(address)).get();
}

std::expected<void, Error> BinderSocket::sendMessage(Identity remoteIdentity, Bytes messagePayload)
{
    return _socket.sendMessage(std::move(remoteIdentity), std::move(messagePayload)).get();
}

std::expected<Message, Error> BinderSocket::recvMessage()
{
    return _socket.recvMessage().get();
}

void BinderSocket::closeConnection(Identity remoteIdentity) noexcept
{
    _socket.closeConnection(std::move(remoteIdentity));
}

}  // namespace sync
}  // namespace ymq
}  // namespace scaler

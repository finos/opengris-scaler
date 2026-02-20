#include "scaler/uv_ymq/sync/binder_socket.h"

#include <utility>

namespace scaler {
namespace uv_ymq {
namespace sync {

BinderSocket::BinderSocket(IOContext& context, Identity identity) noexcept: _socket(context, std::move(identity))
{
}

const Identity& BinderSocket::identity() const noexcept
{
    return _socket.identity();
}

std::expected<Address, scaler::ymq::Error> BinderSocket::bindTo(std::string address)
{
    return _socket.bindTo(std::move(address)).get();
}

std::expected<void, scaler::ymq::Error> BinderSocket::sendMessage(
    Identity remoteIdentity, scaler::ymq::Bytes messagePayload)
{
    return _socket.sendMessage(std::move(remoteIdentity), std::move(messagePayload)).get();
}

std::expected<scaler::ymq::Message, scaler::ymq::Error> BinderSocket::recvMessage()
{
    return _socket.recvMessage().get();
}

void BinderSocket::closeConnection(Identity remoteIdentity) noexcept
{
    _socket.closeConnection(std::move(remoteIdentity));
}

}  // namespace sync
}  // namespace uv_ymq
}  // namespace scaler

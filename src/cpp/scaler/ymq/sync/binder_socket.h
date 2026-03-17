#pragma once

#include <expected>
#include <memory>
#include <string>

#include "scaler/error/error.h"
#include "scaler/ymq/address.h"
#include "scaler/ymq/future/binder_socket.h"
#include "scaler/ymq/io_context.h"
#include "scaler/ymq/message.h"
#include "scaler/ymq/typedefs.h"

namespace scaler {
namespace ymq {
namespace sync {

// Synchronous wrapper for BinderSocket that blocks until operations complete.
class BinderSocket {
public:
    BinderSocket(IOContext& context, Identity identity) noexcept;

    ~BinderSocket() noexcept = default;

    BinderSocket(const BinderSocket&)            = delete;
    BinderSocket& operator=(const BinderSocket&) = delete;

    BinderSocket(BinderSocket&&) noexcept            = default;
    BinderSocket& operator=(BinderSocket&&) noexcept = default;

    const Identity& identity() const noexcept;

    std::expected<Address, Error> bindTo(std::string address);

    std::expected<void, Error> sendMessage(Identity remoteIdentity, Bytes messagePayload);

    std::expected<Message, Error> recvMessage();

    void closeConnection(Identity remoteIdentity) noexcept;

private:
    scaler::ymq::future::BinderSocket _socket;
};

}  // namespace sync
}  // namespace ymq
}  // namespace scaler

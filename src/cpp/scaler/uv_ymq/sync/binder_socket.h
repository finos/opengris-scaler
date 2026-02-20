#pragma once

#include <expected>
#include <memory>
#include <string>

#include "scaler/error/error.h"
#include "scaler/uv_ymq/address.h"
#include "scaler/uv_ymq/future/binder_socket.h"
#include "scaler/uv_ymq/io_context.h"
#include "scaler/uv_ymq/typedefs.h"
#include "scaler/ymq/message.h"

namespace scaler {
namespace uv_ymq {
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

    std::expected<Address, scaler::ymq::Error> bindTo(std::string address);

    std::expected<void, scaler::ymq::Error> sendMessage(Identity remoteIdentity, scaler::ymq::Bytes messagePayload);

    std::expected<scaler::ymq::Message, scaler::ymq::Error> recvMessage();

    void closeConnection(Identity remoteIdentity) noexcept;

private:
    scaler::uv_ymq::future::BinderSocket _socket;
};

}  // namespace sync
}  // namespace uv_ymq
}  // namespace scaler

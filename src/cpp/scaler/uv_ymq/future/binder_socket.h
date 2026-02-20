#pragma once

#include <expected>
#include <future>
#include <memory>
#include <string>

#include "scaler/error/error.h"
#include "scaler/uv_ymq/address.h"
#include "scaler/uv_ymq/binder_socket.h"
#include "scaler/uv_ymq/io_context.h"
#include "scaler/uv_ymq/typedefs.h"
#include "scaler/ymq/message.h"

namespace scaler {
namespace uv_ymq {
namespace future {

// Future-based wrapper for BinderSocket that returns std::future objects.
class BinderSocket {
public:
    BinderSocket(IOContext& context, Identity identity) noexcept;

    ~BinderSocket() noexcept = default;

    BinderSocket(const BinderSocket&)            = delete;
    BinderSocket& operator=(const BinderSocket&) = delete;

    BinderSocket(BinderSocket&&) noexcept            = default;
    BinderSocket& operator=(BinderSocket&&) noexcept = default;

    const Identity& identity() const noexcept;

    std::future<std::expected<Address, scaler::ymq::Error>> bindTo(std::string address);

    std::future<std::expected<void, scaler::ymq::Error>> sendMessage(
        Identity remoteIdentity, scaler::ymq::Bytes messagePayload);

    std::future<std::expected<scaler::ymq::Message, scaler::ymq::Error>> recvMessage();

    void closeConnection(Identity remoteIdentity) noexcept;

private:
    scaler::uv_ymq::BinderSocket _socket;
};

}  // namespace future
}  // namespace uv_ymq
}  // namespace scaler

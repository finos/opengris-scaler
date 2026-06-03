#pragma once

#include <expected>
#include <future>
#include <memory>
#include <optional>
#include <string>

#include "scaler/error/error.h"
#include "scaler/ymq/address.h"
#include "scaler/ymq/binder_socket.h"
#include "scaler/ymq/io_context.h"
#include "scaler/ymq/message.h"
#include "scaler/ymq/typedefs.h"

namespace scaler {
namespace ymq {
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

    std::future<std::expected<Address, Error>> bindTo(std::string address);

    std::future<std::expected<void, Error>> sendMessage(Identity remoteIdentity, Bytes messagePayload);

    void sendMulticastMessage(Bytes messagePayload, std::optional<Identity> remotePrefix = std::nullopt) noexcept;

    std::future<std::expected<Message, Error>> recvMessage();

    void closeConnection(Identity remoteIdentity) noexcept;

private:
    scaler::ymq::BinderSocket _socket;
};

}  // namespace future
}  // namespace ymq
}  // namespace scaler

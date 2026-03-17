#pragma once

#include <chrono>
#include <expected>
#include <memory>
#include <string>

#include "scaler/error/error.h"
#include "scaler/ymq/configuration.h"
#include "scaler/ymq/future/connector_socket.h"
#include "scaler/ymq/io_context.h"
#include "scaler/ymq/message.h"
#include "scaler/ymq/typedefs.h"

namespace scaler {
namespace ymq {
namespace sync {

// Synchronous wrapper for ConnectorSocket that blocks until operations complete.
class ConnectorSocket {
public:
    static std::expected<ConnectorSocket, Error> connect(
        IOContext& context,
        Identity identity,
        std::string address,
        size_t maxRetryTimes                     = defaultClientMaxRetryTimes,
        std::chrono::milliseconds initRetryDelay = defaultClientInitRetryDelay);

    static std::expected<std::pair<ConnectorSocket, Address>, Error> bind(
        IOContext& context, Identity identity, std::string address);

    ~ConnectorSocket() noexcept = default;

    ConnectorSocket(const ConnectorSocket&)            = delete;
    ConnectorSocket& operator=(const ConnectorSocket&) = delete;

    ConnectorSocket(ConnectorSocket&&) noexcept            = default;
    ConnectorSocket& operator=(ConnectorSocket&&) noexcept = default;

    const Identity& identity() const noexcept;

    std::expected<void, Error> sendMessage(Bytes messagePayload);

    std::expected<Message, Error> recvMessage();

private:
    ConnectorSocket(future::ConnectorSocket socket) noexcept;

    scaler::ymq::future::ConnectorSocket _socket;
};

}  // namespace sync
}  // namespace ymq
}  // namespace scaler

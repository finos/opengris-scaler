#pragma once

#include <chrono>
#include <expected>
#include <memory>
#include <string>

#include "scaler/error/error.h"
#include "scaler/uv_ymq/configuration.h"
#include "scaler/uv_ymq/future/connector_socket.h"
#include "scaler/uv_ymq/io_context.h"
#include "scaler/uv_ymq/typedefs.h"
#include "scaler/ymq/message.h"

namespace scaler {
namespace uv_ymq {
namespace sync {

// Synchronous wrapper for ConnectorSocket that blocks until operations complete.
class ConnectorSocket {
public:
    static std::expected<ConnectorSocket, scaler::ymq::Error> init(
        IOContext& context,
        Identity identity,
        std::string address,
        size_t maxRetryTimes                     = defaultClientMaxRetryTimes,
        std::chrono::milliseconds initRetryDelay = defaultClientInitRetryDelay);

    ~ConnectorSocket() noexcept = default;

    ConnectorSocket(const ConnectorSocket&)            = delete;
    ConnectorSocket& operator=(const ConnectorSocket&) = delete;

    ConnectorSocket(ConnectorSocket&&) noexcept            = default;
    ConnectorSocket& operator=(ConnectorSocket&&) noexcept = default;

    const Identity& identity() const noexcept;

    std::expected<void, scaler::ymq::Error> sendMessage(scaler::ymq::Bytes messagePayload);

    std::expected<scaler::ymq::Message, scaler::ymq::Error> recvMessage();

private:
    ConnectorSocket(scaler::uv_ymq::future::ConnectorSocket socket) noexcept;

    scaler::uv_ymq::future::ConnectorSocket _socket;
};

}  // namespace sync
}  // namespace uv_ymq
}  // namespace scaler

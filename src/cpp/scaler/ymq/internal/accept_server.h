#pragma once

#include <expected>
#include <memory>
#include <optional>
#include <variant>

#include "scaler/logging/logging.h"
#include "scaler/wrapper/uv/error.h"
#include "scaler/wrapper/uv/loop.h"
#include "scaler/wrapper/uv/pipe.h"
#include "scaler/wrapper/uv/tcp.h"
#include "scaler/ymq/address.h"
#include "scaler/ymq/internal/client.h"

namespace scaler {
namespace ymq {
namespace internal {

// A server that accepts incoming connections.
//
// Binds to the specified address and calls the callback when a new connection arrives.
class AcceptServer {
public:
    using ConnectionCallback = scaler::utility::MoveOnlyFunction<void(Client)>;

    // Create a server bound to and listening on `address`. Returns an error instead of an
    // instance when binding or listening fails (e.g. EADDRINUSE when the address is in use).
    static std::expected<AcceptServer, scaler::wrapper::uv::Error> init(
        scaler::wrapper::uv::Loop& loop, Address address, ConnectionCallback onConnectionCallback) noexcept;

    ~AcceptServer() noexcept;

    AcceptServer(const AcceptServer&)            = delete;
    AcceptServer& operator=(const AcceptServer&) = delete;

    AcceptServer(AcceptServer&&) noexcept            = default;
    AcceptServer& operator=(AcceptServer&&) noexcept = default;

    Address address() const noexcept;

    void disconnect() noexcept;

private:
    AcceptServer() noexcept = default;

    using Server = std::variant<scaler::wrapper::uv::TCPServer, scaler::wrapper::uv::PipeServer>;

    // State is heap-allocated to provide a stable memory for callbacks if the client is std::move'd or freed.

    struct State {
        scaler::wrapper::uv::Loop& _loop;

        ConnectionCallback _onConnectionCallback;

        std::optional<Server> _server;

        // Set when the transport is WebSocket; used to reconstruct the address() return value.
        std::optional<WebSocketAddress> _webSocketAddress;

        State(
            scaler::wrapper::uv::Loop& loop,
            ConnectionCallback onConnectionCallback,
            Server server,
            std::optional<WebSocketAddress> webSocketAddress) noexcept;
    };

    std::shared_ptr<State> _state;

    static void onConnection(
        std::shared_ptr<State> state, std::expected<void, scaler::wrapper::uv::Error> result) noexcept;
};

}  // namespace internal
}  // namespace ymq
}  // namespace scaler

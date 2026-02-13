#pragma once

#include <memory>
#include <optional>
#include <variant>

#include "scaler/logging/logging.h"
#include "scaler/uv_ymq/address.h"
#include "scaler/uv_ymq/typedefs.h"
#include "scaler/wrapper/uv/loop.h"
#include "scaler/wrapper/uv/pipe.h"
#include "scaler/wrapper/uv/tcp.h"

namespace scaler {
namespace uv_ymq {
namespace internal {

// A server that accepts incoming connections.
//
// Binds to the specified address and calls the callback when a new connection arrives.
class AcceptServer {
public:
    using ConnectionCallback = scaler::utility::MoveOnlyFunction<void(Client)>;

    AcceptServer(scaler::wrapper::uv::Loop& loop, Address address, ConnectionCallback onConnectionCallback) noexcept;

    ~AcceptServer() noexcept;

    AcceptServer(const AcceptServer&)            = delete;
    AcceptServer& operator=(const AcceptServer&) = delete;

    AcceptServer(AcceptServer&&) noexcept            = default;
    AcceptServer& operator=(AcceptServer&&) noexcept = default;

    Address address() const noexcept;

    void disconnect() noexcept;

private:
    using Server = std::variant<scaler::wrapper::uv::TCPServer, scaler::wrapper::uv::PipeServer>;

    // State is heap-allocated to provide a stable memory for callbacks if the client is std::move'd or freed.

    struct State {
        scaler::wrapper::uv::Loop& _loop;

        ConnectionCallback _onConnectionCallback;

        std::optional<Server> _server;

        State(scaler::wrapper::uv::Loop& loop, ConnectionCallback onConnectionCallback, Server server) noexcept;
    };

    std::shared_ptr<State> _state;

    static void onConnection(
        std::shared_ptr<State> state, std::expected<void, scaler::wrapper::uv::Error> result) noexcept;
};

}  // namespace internal
}  // namespace uv_ymq
}  // namespace scaler

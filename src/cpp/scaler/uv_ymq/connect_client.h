#pragma once

#include <chrono>
#include <expected>
#include <memory>
#include <string>
#include <variant>

#include "scaler/logging/logging.h"
#include "scaler/uv_ymq/address.h"
#include "scaler/uv_ymq/configuration.h"
#include "scaler/uv_ymq/event_loop_thread.h"
#include "scaler/uv_ymq/typedefs.h"
#include "scaler/wrapper/uv/loop.h"
#include "scaler/wrapper/uv/timer.h"

namespace scaler {
namespace uv_ymq {

// A not yet established client to a remote server.
//
// Tries to connect to the address up to maxRetryTimes, calling the callback on success or failure.
class ConnectClient {
public:
    using ConnectCallback = scaler::utility::MoveOnlyFunction<void(std::expected<Client, scaler::ymq::Error>)>;

    ConnectClient(
        scaler::wrapper::uv::Loop& loop,
        Address address,
        ConnectCallback onConnectCallback,
        size_t maxRetryTimes                     = defaultClientMaxRetryTimes,
        std::chrono::milliseconds initRetryDelay = defaultClientInitRetryDelay) noexcept;

    ~ConnectClient() noexcept;

    ConnectClient(const ConnectClient&)            = delete;
    ConnectClient& operator=(const ConnectClient&) = delete;

    ConnectClient(ConnectClient&&) noexcept            = default;
    ConnectClient& operator=(ConnectClient&&) noexcept = default;

    void disconnect() noexcept;

private:
    // State is heap-allocated to provide a stable memory for callbacks if the client is std::move'd or freed.

    struct State {
        scaler::ymq::Logger _logger {};

        scaler::wrapper::uv::Loop& _loop;

        Address _address;

        ConnectCallback _onConnectCallback;

        std::optional<Client> _client {};
        std::optional<scaler::wrapper::uv::ConnectRequest> _connectRequest {};

        size_t _maxRetryTimes;
        size_t _retryTimes {0};
        std::chrono::milliseconds _initRetryDelay;
        std::optional<scaler::wrapper::uv::Timer> _retryTimer {};

        State(
            scaler::wrapper::uv::Loop& loop,
            Address address,
            ConnectCallback onConnectCallback,
            size_t maxRetryTimes,
            std::chrono::milliseconds initRetryDelay) noexcept;
    };

    std::shared_ptr<State> _state;

    static void tryConnect(std::shared_ptr<State> state) noexcept;

    static void onConnect(
        std::shared_ptr<State> state, std::expected<void, scaler::wrapper::uv::Error> result) noexcept;

    static void retry(std::shared_ptr<State> state) noexcept;
};

}  // namespace uv_ymq
}  // namespace scaler

#include "scaler/uv_ymq/connect_client.h"

#include <chrono>
#include <functional>
#include <string>

#include "scaler/wrapper/uv/pipe.h"
#include "scaler/wrapper/uv/socket_address.h"
#include "scaler/wrapper/uv/tcp.h"

namespace scaler {
namespace uv_ymq {

ConnectClient::ConnectClient(
    scaler::wrapper::uv::Loop& loop,
    Address address,
    ConnectCallback onConnectCallback,
    size_t maxRetryTimes,
    std::chrono::milliseconds initRetryDelay) noexcept
    : _state(
          std::make_shared<State>(
              loop, std::move(address), std::move(onConnectCallback), maxRetryTimes, initRetryDelay))
{
    tryConnect(_state);
}

ConnectClient::State::State(
    scaler::wrapper::uv::Loop& loop,
    Address addr,
    ConnectCallback callback,
    size_t maxRetries,
    std::chrono::milliseconds delay) noexcept
    : _loop(loop)
    , _address(std::move(addr))
    , _onConnectCallback(std::move(callback))
    , _maxRetryTimes(maxRetries)
    , _initRetryDelay(delay)
{
}

ConnectClient::~ConnectClient() noexcept
{
    disconnect();
}

void ConnectClient::disconnect() noexcept
{
    if (_state->_retryTimer.has_value()) {
        _state->_retryTimer->stop();
        _state->_retryTimer.reset();
    }

    _state->_connectRequest.reset();
    _state->_client.reset();
}

void ConnectClient::tryConnect(std::shared_ptr<State> state) noexcept
{
    switch (state->_address.type()) {
        case Address::Type::TCP: {
            auto tcpClient = UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPSocket::init(state->_loop));

            state->_connectRequest = UV_EXIT_ON_ERROR(
                tcpClient.connect(state->_address.asTCP(), std::bind_front(&ConnectClient::onConnect, state)));

            state->_client = std::move(tcpClient);
            break;
        }
        case Address::Type::IPC: {
            auto ipcClient = UV_EXIT_ON_ERROR(scaler::wrapper::uv::Pipe::init(state->_loop, false));

            state->_connectRequest = UV_EXIT_ON_ERROR(
                ipcClient.connect(state->_address.asIPC(), std::bind_front(&ConnectClient::onConnect, state)));

            state->_client = std::move(ipcClient);
            break;
        }
        default: std::unreachable();
    }
}

void ConnectClient::onConnect(
    std::shared_ptr<State> state, std::expected<void, scaler::wrapper::uv::Error> result) noexcept
{
    if (!result.has_value()) {
        if (result.error() == scaler::wrapper::uv::Error {UV_ECANCELED}) {
            state->_onConnectCallback(
                std::unexpected(scaler::ymq::Error(scaler::ymq::Error::ErrorCode::IOSocketStopRequested)));
            state->_onConnectCallback = {};  // immediately release the callback resources
        } else {
            retry(std::move(state));
        }
        return;
    }

    state->_onConnectCallback(std::move(state->_client.value()));
    state->_onConnectCallback = {};
}

void ConnectClient::retry(std::shared_ptr<State> state) noexcept
{
    ++state->_retryTimes;

    if (state->_retryTimes > state->_maxRetryTimes) {
        state->_logger.log(
            scaler::ymq::Logger::LoggingLevel::error, "Retried times has reached maximum: ", state->_maxRetryTimes);
        state->_client = std::nullopt;

        state->_onConnectCallback(
            std::unexpected(
                scaler::ymq::Error(
                    scaler::ymq::Error::ErrorCode::ConnectorSocketClosedByRemoteEnd,
                    "Retried times has reached maximum",
                    state->_maxRetryTimes)));
        state->_onConnectCallback = {};

        return;
    }

    state->_logger.log(scaler::ymq::Logger::LoggingLevel::debug, "Client retrying ", state->_retryTimes, " time(s)");

    std::chrono::milliseconds delay {state->_initRetryDelay.count() << state->_retryTimes};

    state->_retryTimer = UV_EXIT_ON_ERROR(scaler::wrapper::uv::Timer::init(state->_loop));
    state->_retryTimer->start(delay, std::nullopt, std::bind_front(&ConnectClient::tryConnect, std::move(state)));
}

}  // namespace uv_ymq
}  // namespace scaler

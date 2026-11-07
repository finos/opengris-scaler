#include <gtest/gtest.h>

#include <chrono>
#include <expected>
#include <optional>
#include <string>

#include "scaler/error/error.h"
#include "scaler/wrapper/uv/error.h"
#include "scaler/wrapper/uv/loop.h"
#include "scaler/wrapper/uv/tcp.h"
#include "scaler/ymq/address.h"
#include "scaler/ymq/internal/connect_client.h"

class YMQConnectClientTest: public ::testing::Test {};

TEST_F(YMQConnectClientTest, ConnectClient)
{
    // Successfully connect to a temporary TCP server

    constexpr int maxRetryTimes = scaler::ymq::defaultClientMaxRetryTimes;
    constexpr std::chrono::milliseconds initRetryDelay {10};

    scaler::wrapper::uv::Loop loop = UV_EXIT_ON_ERROR(scaler::wrapper::uv::Loop::init());

    const auto listenAddress = scaler::ymq::Address::fromString("tcp://127.0.0.1:0").value();

    // Create a temporary TCP server
    scaler::wrapper::uv::TCPServer server = UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPServer::init(loop));
    UV_EXIT_ON_ERROR(server.bind(listenAddress.asTCP(), uv_tcp_flags(0)));
    UV_EXIT_ON_ERROR(server.listen(16, [&](std::expected<void, scaler::wrapper::uv::Error>) {
        scaler::wrapper::uv::TCPSocket acceptingSocket = UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPSocket::init(loop));
        UV_EXIT_ON_ERROR(server.accept(acceptingSocket));
    }));

    bool callbackCalled = false;

    auto onConnectCallback = [&](std::expected<scaler::ymq::internal::Client, scaler::ymq::Error> result) {
        ASSERT_TRUE(result.has_value());
        callbackCalled = true;
    };

    // Get the actual bound address (since we used port 0)
    scaler::ymq::Address connectAddress {UV_EXIT_ON_ERROR(server.getSockName())};

    auto connectClient = scaler::ymq::internal::ConnectClient::init(
                             loop, connectAddress, onConnectCallback, maxRetryTimes, initRetryDelay)
                             .value();

    while (!callbackCalled) {
        loop.run(UV_RUN_ONCE);
    }
}

TEST_F(YMQConnectClientTest, ConnectClientFailure)
{
    // Simulate a connection failure

    constexpr int maxRetryTimes = scaler::ymq::defaultClientMaxRetryTimes;
    constexpr std::chrono::milliseconds initRetryDelay {10};

    scaler::wrapper::uv::Loop loop = UV_EXIT_ON_ERROR(scaler::wrapper::uv::Loop::init());

    // Port 49151 is IANA reserved, hopefully never assigned
    const auto address = scaler::ymq::Address::fromString("tcp://127.0.0.1:49151").value();

    bool callbackCalled = false;

    auto onConnectCallback = [&](std::expected<scaler::ymq::internal::Client, scaler::ymq::Error> result) {
        ASSERT_FALSE(result.has_value());
        ASSERT_EQ(result.error()._errorCode, scaler::ymq::Error::ErrorCode::ConnectorSocketClosedByRemoteEnd);
        callbackCalled = true;
    };

    auto connectClient =
        scaler::ymq::internal::ConnectClient::init(loop, address, onConnectCallback, maxRetryTimes, initRetryDelay)
            .value();

    loop.run();

    ASSERT_TRUE(callbackCalled);
}

TEST_F(YMQConnectClientTest, ConnectClientDisconnect)
{
    // Cancel an ongoing connection

    constexpr int maxRetryTimes = scaler::ymq::defaultClientMaxRetryTimes;
    constexpr std::chrono::milliseconds initRetryDelay {10};

    scaler::wrapper::uv::Loop loop = UV_EXIT_ON_ERROR(scaler::wrapper::uv::Loop::init());

    // 192.0.2.0/24 is non-routable. connect() usually timeouts after a few seconds.
    const auto address = scaler::ymq::Address::fromString("tcp://192.0.2.1:9999").value();

    bool callbackCalled = false;

    auto onConnectCallback = [&](std::expected<scaler::ymq::internal::Client, scaler::ymq::Error> result) {
        ASSERT_FALSE(result.has_value());
        ASSERT_EQ(result.error()._errorCode, scaler::ymq::Error::ErrorCode::SocketStopRequested);
        callbackCalled = true;
    };

    auto connectClient =
        scaler::ymq::internal::ConnectClient::init(loop, address, onConnectCallback, maxRetryTimes, initRetryDelay)
            .value();

    // Set up a timer to disconnect after a short delay
    scaler::wrapper::uv::Timer disconnectTimer = UV_EXIT_ON_ERROR(scaler::wrapper::uv::Timer::init(loop));
    UV_EXIT_ON_ERROR(
        disconnectTimer.start(std::chrono::milliseconds {50}, std::nullopt, [&]() { connectClient.disconnect(); }));

    while (!callbackCalled) {
        loop.run(UV_RUN_ONCE);
    }
}

TEST_F(YMQConnectClientTest, ConnectClientDisconnectDuringWebSocketUpgrade)
{
    // A WebSocket upgrade's read outlives the disconnect, and a failure it then reports must not start a retry.

    constexpr int maxRetryTimes = 1;
    constexpr std::chrono::milliseconds initRetryDelay {1};

    scaler::wrapper::uv::Loop loop = UV_EXIT_ON_ERROR(scaler::wrapper::uv::Loop::init());

    const auto listenAddress = scaler::ymq::Address::fromString("tcp://127.0.0.1:0").value();

    // A temporary TCP server that never answers the upgrade
    std::optional<scaler::wrapper::uv::TCPServer> server {UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPServer::init(loop))};
    std::optional<scaler::wrapper::uv::TCPSocket> upgradingSocket;
    bool upgradeRequestReceived = false;

    UV_EXIT_ON_ERROR(server->bind(listenAddress.asTCP(), uv_tcp_flags(0)));
    UV_EXIT_ON_ERROR(server->listen(16, [&](std::expected<void, scaler::wrapper::uv::Error>) {
        upgradingSocket.emplace(UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPSocket::init(loop)));
        UV_EXIT_ON_ERROR(server->accept(*upgradingSocket));
        UV_EXIT_ON_ERROR(upgradingSocket->readStart([&](auto) { upgradeRequestReceived = true; }));
    }));

    const std::string tcpAddress = scaler::ymq::Address {UV_EXIT_ON_ERROR(server->getSockName())}.toString().value();
    const auto address = scaler::ymq::Address::fromString("ws" + tcpAddress.substr(tcpAddress.find(':'))).value();

    std::optional<scaler::ymq::Error::ErrorCode> connectError;

    auto onConnectCallback = [&](std::expected<scaler::ymq::internal::Client, scaler::ymq::Error> result) {
        ASSERT_FALSE(result.has_value());
        connectError = result.error()._errorCode;
    };

    auto connectClient =
        scaler::ymq::internal::ConnectClient::init(loop, address, onConnectCallback, maxRetryTimes, initRetryDelay)
            .value();

    while (!upgradeRequestReceived) {
        loop.run(UV_RUN_ONCE);
    }

    connectClient.disconnect();

    // Fail the upgrade's read, with no server left that a retry could reach.
    server.reset();
    upgradingSocket.reset();

    while (!connectError.has_value()) {
        loop.run(UV_RUN_ONCE);
    }

    ASSERT_EQ(*connectError, scaler::ymq::Error::ErrorCode::SocketStopRequested);
}

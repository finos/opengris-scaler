#include <gtest/gtest.h>

#include <chrono>
#include <expected>

#include "scaler/error/error.h"
#include "scaler/uv_ymq/address.h"
#include "scaler/uv_ymq/connecting_client.h"
#include "scaler/wrapper/uv/error.h"
#include "scaler/wrapper/uv/loop.h"
#include "scaler/wrapper/uv/tcp.h"

class UVYMQConnectingClientTest: public ::testing::Test {};

TEST_F(UVYMQConnectingClientTest, ConnectingClient)
{
    // Successfully connect to a temporary TCP server

    constexpr int MAX_RETRY_TIMES = scaler::uv_ymq::DEFAULT_CLIENT_MAX_RETRY_TIMES;
    constexpr std::chrono::milliseconds INIT_RETRY_DELAY {10};

    scaler::wrapper::uv::Loop loop = UV_EXIT_ON_ERROR(scaler::wrapper::uv::Loop::init());

    const auto LISTEN_ADDRESS = scaler::uv_ymq::Address::fromString("tcp://127.0.0.1:0").value();

    // Create a temporary TCP server
    scaler::wrapper::uv::TCPServer server = UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPServer::init(loop));
    UV_EXIT_ON_ERROR(server.bind(LISTEN_ADDRESS.asTCP(), uv_tcp_flags(0)));
    UV_EXIT_ON_ERROR(server.listen(16, [&](std::expected<void, scaler::wrapper::uv::Error>) {
        scaler::wrapper::uv::TCPSocket acceptingSocket = UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPSocket::init(loop));
        UV_EXIT_ON_ERROR(server.accept(acceptingSocket));
    }));

    bool callbackCalled = false;

    auto onConnectCallback = [&](std::expected<scaler::uv_ymq::Client, scaler::ymq::Error> result) {
        ASSERT_TRUE(result.has_value());
        callbackCalled = true;
    };

    // Get the actual bound address (since we used port 0)
    scaler::uv_ymq::Address connectAddress {UV_EXIT_ON_ERROR(server.getSockName())};

    scaler::uv_ymq::ConnectingClient connectingClient(
        loop, connectAddress, onConnectCallback, MAX_RETRY_TIMES, INIT_RETRY_DELAY);

    while (!callbackCalled) {
        loop.run(UV_RUN_ONCE);
    }
}

TEST_F(UVYMQConnectingClientTest, ConnectingClientFailure)
{
    // Simulate a connection failure

    constexpr int MAX_RETRY_TIMES = scaler::uv_ymq::DEFAULT_CLIENT_MAX_RETRY_TIMES;
    constexpr std::chrono::milliseconds INIT_RETRY_DELAY {10};

    scaler::wrapper::uv::Loop loop = UV_EXIT_ON_ERROR(scaler::wrapper::uv::Loop::init());

    // Port 49151 is IANA reserved, hopefully never assigned
    const auto ADDRESS = scaler::uv_ymq::Address::fromString("tcp://127.0.0.1:49151").value();

    bool callbackCalled = false;

    auto onConnectCallback = [&](std::expected<scaler::uv_ymq::Client, scaler::ymq::Error> result) {
        ASSERT_FALSE(result.has_value());
        ASSERT_EQ(result.error()._errorCode, scaler::ymq::Error::ErrorCode::ConnectorSocketClosedByRemoteEnd);
        callbackCalled = true;
    };

    scaler::uv_ymq::ConnectingClient connectingClient(
        loop, ADDRESS, onConnectCallback, MAX_RETRY_TIMES, INIT_RETRY_DELAY);

    loop.run();

    ASSERT_TRUE(callbackCalled);
}

TEST_F(UVYMQConnectingClientTest, ConnectingClientDisconnect)
{
    // Cancel an ongoing connection

    constexpr int MAX_RETRY_TIMES = scaler::uv_ymq::DEFAULT_CLIENT_MAX_RETRY_TIMES;
    constexpr std::chrono::milliseconds INIT_RETRY_DELAY {10};

    scaler::wrapper::uv::Loop loop = UV_EXIT_ON_ERROR(scaler::wrapper::uv::Loop::init());

    // 192.0.2.0/24 is non-routable. connect() usually timeouts after a few seconds.
    const auto ADDRESS = scaler::uv_ymq::Address::fromString("tcp://192.0.2.1:9999").value();

    bool callbackCalled = false;

    auto onConnectCallback = [&](std::expected<scaler::uv_ymq::Client, scaler::ymq::Error> result) {
        ASSERT_FALSE(result.has_value());
        ASSERT_EQ(result.error()._errorCode, scaler::ymq::Error::ErrorCode::IOSocketStopRequested);
        callbackCalled = true;
    };

    scaler::uv_ymq::ConnectingClient connectingClient(
        loop, ADDRESS, onConnectCallback, MAX_RETRY_TIMES, INIT_RETRY_DELAY);

    // Set up a timer to disconnect after a short delay
    scaler::wrapper::uv::Timer disconnectTimer = UV_EXIT_ON_ERROR(scaler::wrapper::uv::Timer::init(loop));
    UV_EXIT_ON_ERROR(
        disconnectTimer.start(std::chrono::milliseconds {50}, std::nullopt, [&]() { connectingClient.disconnect(); }));

    while (!callbackCalled) {
        loop.run(UV_RUN_ONCE);
    }
}

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <expected>
#include <mutex>
#include <set>
#include <string>
#include <thread>

#include "scaler/error/error.h"
#include "scaler/uv_ymq/address.h"
#include "scaler/uv_ymq/connecting_client.h"
#include "scaler/uv_ymq/event_loop_thread.h"
#include "scaler/uv_ymq/io_context.h"
#include "scaler/wrapper/uv/error.h"
#include "scaler/wrapper/uv/loop.h"
#include "scaler/wrapper/uv/tcp.h"

class UVYMQTest: public ::testing::Test {
protected:
};

TEST_F(UVYMQTest, Address)
{
    // Valid addresses

    std::expected<scaler::uv_ymq::Address, scaler::ymq::Error> address =
        scaler::uv_ymq::Address::fromString("tcp://127.0.0.1:8080");
    ASSERT_TRUE(address.has_value());
    ASSERT_EQ(address->type(), scaler::uv_ymq::Address::Type::TCP);

    address = scaler::uv_ymq::Address::fromString("tcp://2001:db8::1:1211");
    ASSERT_TRUE(address.has_value());
    ASSERT_EQ(address->type(), scaler::uv_ymq::Address::Type::TCP);

    address = scaler::uv_ymq::Address::fromString("tcp://::1:8080");
    ASSERT_TRUE(address.has_value());
    ASSERT_EQ(address->type(), scaler::uv_ymq::Address::Type::TCP);

    address = scaler::uv_ymq::Address::fromString("ipc://some_ipc_socket_name");
    ASSERT_TRUE(address.has_value());
    ASSERT_EQ(address->type(), scaler::uv_ymq::Address::Type::IPC);
    ASSERT_EQ(std::get<std::string>(address->value()), "some_ipc_socket_name");

    // Invalid addresses

    address = scaler::uv_ymq::Address::fromString("http://127.0.0.1:8080");
    ASSERT_FALSE(address.has_value());

    address = scaler::uv_ymq::Address::fromString("127.0.0.1:8080");
    ASSERT_FALSE(address.has_value());

    address = scaler::uv_ymq::Address::fromString("tcp://127.0.0.1");
    ASSERT_FALSE(address.has_value());

    address = scaler::uv_ymq::Address::fromString("");
    ASSERT_FALSE(address.has_value());
}

TEST_F(UVYMQTest, EventLoopThread)
{
    const size_t N_TASKS = 3;

    std::atomic<int> nTimesCalled {0};

    {
        scaler::uv_ymq::EventLoopThread thread {};

        for (size_t i = 0; i < N_TASKS; ++i) {
            thread.executeThreadSafe([&]() { ++nTimesCalled; });
        }

        // Wait for the loop to process the callbacks
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    ASSERT_EQ(nTimesCalled, N_TASKS);
}

TEST_F(UVYMQTest, IOContext)
{
    const size_t N_TASKS   = 10;
    const size_t N_THREADS = 4;

    std::set<std::thread::id> uniqueThreadIds {};
    std::mutex uniqueThreadIdsMutex {};

    {
        scaler::uv_ymq::IOContext context {N_THREADS};

        // Execute tasks on different threads in round-robin fashion
        for (size_t i = 0; i < N_TASKS; ++i) {
            context.nextThread().executeThreadSafe([&]() {
                std::lock_guard<std::mutex> lock(uniqueThreadIdsMutex);
                uniqueThreadIds.insert(std::this_thread::get_id());
            });
        }

        // Wait for the loops to process the callbacks
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    ASSERT_EQ(uniqueThreadIds.size(), N_THREADS);
}

TEST_F(UVYMQTest, ConnectingClient)
{
    constexpr int MAX_RETRY_TIMES = scaler::uv_ymq::DEFAULT_MAX_RETRY_TIMES;
    constexpr std::chrono::milliseconds INIT_RETRY_DELAY {10};

    scaler::wrapper::uv::Loop loop = UV_EXIT_ON_ERROR(scaler::wrapper::uv::Loop::init());

    // Successfully connect to a temporary TCP server
    {
        const auto LISTEN_ADDRESS = scaler::uv_ymq::Address::fromString("tcp://127.0.0.1:0").value();

        // Create a temporary TCP server
        scaler::wrapper::uv::TCPServer server = UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPServer::init(loop));
        UV_EXIT_ON_ERROR(server.bind(LISTEN_ADDRESS.asTCP(), uv_tcp_flags(0)));
        UV_EXIT_ON_ERROR(server.listen(16, [&](std::expected<void, scaler::wrapper::uv::Error>) {
            scaler::wrapper::uv::TCPSocket acceptingSocket =
                UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPSocket::init(loop));
            UV_EXIT_ON_ERROR(server.accept(acceptingSocket));
        }));

        bool callbackCalled = false;

        auto onConnectCallback = [&](std::expected<scaler::uv_ymq::Client, scaler::ymq::Error> result) {
            ASSERT_TRUE(result.has_value());
            callbackCalled = true;
        };

        scaler::uv_ymq::Address connectAddress {UV_EXIT_ON_ERROR(server.getSockName())};

        scaler::uv_ymq::ConnectingClient connectingClient(
            loop, connectAddress, onConnectCallback, MAX_RETRY_TIMES, INIT_RETRY_DELAY);

        while (!callbackCalled) {
            loop.run(UV_RUN_ONCE);
        }
    }

    // Fail to connect
    {
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

    // Cancelling connection
    {
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
        UV_EXIT_ON_ERROR(disconnectTimer.start(
            std::chrono::milliseconds {50}, std::nullopt, [&]() { connectingClient.disconnect(); }));

        while (!callbackCalled) {
            loop.run(UV_RUN_ONCE);
        }
    }
}

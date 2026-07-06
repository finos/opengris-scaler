#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <expected>
#include <mutex>
#include <set>
#include <thread>

#include "scaler/error/error.h"
#include "scaler/ymq/address.h"
#include "scaler/ymq/io_context.h"
#include "tests/cpp/ymq/common/testing.h"

class YMQTest: public ::testing::Test {};

TEST_F(YMQTest, Address)
{
    // Valid addresses

    std::expected<scaler::ymq::Address, scaler::ymq::Error> address =
        scaler::ymq::Address::fromString("tcp://127.0.0.1:8080");
    ASSERT_TRUE(address.has_value());
    ASSERT_EQ(address->type(), scaler::ymq::Address::Type::TCP);
    ASSERT_FALSE(address->secure());

    address = scaler::ymq::Address::fromString("tcp://2001:db8::1:1211");
    ASSERT_TRUE(address.has_value());
    ASSERT_EQ(address->type(), scaler::ymq::Address::Type::TCP);
    ASSERT_FALSE(address->secure());

    address = scaler::ymq::Address::fromString("tcp://::1:8080");
    ASSERT_TRUE(address.has_value());
    ASSERT_EQ(address->type(), scaler::ymq::Address::Type::TCP);
    ASSERT_FALSE(address->secure());

    address = scaler::ymq::Address::fromString("tls://127.0.0.1:8080");
    ASSERT_TRUE(address.has_value());
    ASSERT_EQ(address->type(), scaler::ymq::Address::Type::TCP);
    ASSERT_TRUE(address->secure());

    address = scaler::ymq::Address::fromString("tls://2001:db8::1:1211");
    ASSERT_TRUE(address.has_value());
    ASSERT_EQ(address->type(), scaler::ymq::Address::Type::TCP);
    ASSERT_TRUE(address->secure());

    address = scaler::ymq::Address::fromString("ipc://some_ipc_socket_name");
    ASSERT_TRUE(address.has_value());
    ASSERT_EQ(address->type(), scaler::ymq::Address::Type::IPC);
    ASSERT_FALSE(address->secure());
    ASSERT_EQ(std::get<std::string>(address->value()), "some_ipc_socket_name");

    address = scaler::ymq::Address::fromString("ws://127.0.0.1:8765/");
    ASSERT_TRUE(address.has_value());
    ASSERT_EQ(address->type(), scaler::ymq::Address::Type::WebSocket);
    ASSERT_FALSE(address->secure());
    ASSERT_EQ(address->asWebSocket().path, "/");

    address = scaler::ymq::Address::fromString("ws://127.0.0.1:9000/ymq");
    ASSERT_TRUE(address.has_value());
    ASSERT_EQ(address->asWebSocket().path, "/ymq");

    address = scaler::ymq::Address::fromString("wss://127.0.0.1:443/");
    ASSERT_TRUE(address.has_value());
    ASSERT_EQ(address->type(), scaler::ymq::Address::Type::WebSocket);
    ASSERT_TRUE(address->secure());

    // Invalid addresses

    address = scaler::ymq::Address::fromString("http://127.0.0.1:8080");
    ASSERT_FALSE(address.has_value());

    address = scaler::ymq::Address::fromString("127.0.0.1:8080");
    ASSERT_FALSE(address.has_value());

    address = scaler::ymq::Address::fromString("tcp://127.0.0.1");
    ASSERT_FALSE(address.has_value());

    address = scaler::ymq::Address::fromString("ws://127.0.0.1");
    ASSERT_FALSE(address.has_value());

    address = scaler::ymq::Address::fromString("");
    ASSERT_FALSE(address.has_value());

    // Address::toString()

    address = scaler::ymq::Address::fromString("tcp://127.0.0.1:9000");
    ASSERT_TRUE(address.has_value());
    ASSERT_EQ(address->toString().value(), "tcp://127.0.0.1:9000");

    address = scaler::ymq::Address::fromString("tls://127.0.0.1:9000");
    ASSERT_TRUE(address.has_value());
    ASSERT_EQ(address->toString().value(), "tls://127.0.0.1:9000");

    address = scaler::ymq::Address::fromString("ipc://some_ipc_socket_name");
    ASSERT_TRUE(address.has_value());
    ASSERT_EQ(address->toString().value(), "ipc://some_ipc_socket_name");

    address = scaler::ymq::Address::fromString("ws://127.0.0.1:9000/");
    ASSERT_TRUE(address.has_value());
    ASSERT_EQ(address->toString().value(), "ws://127.0.0.1:9000/");

    address = scaler::ymq::Address::fromString("wss://127.0.0.1:443/ymq");
    ASSERT_TRUE(address.has_value());
    ASSERT_EQ(address->toString().value(), "wss://127.0.0.1:443/ymq");
}

TEST_F(YMQTest, IOContext)
{
    const size_t nTasks   = 10;
    const size_t nThreads = 4;

    std::set<std::thread::id> uniqueThreadIds {};
    std::mutex uniqueThreadIdsMutex {};

    {
        scaler::ymq::IOContext context {nThreads};

        // Execute tasks on different threads in round-robin fashion
        for (size_t i = 0; i < nTasks; ++i) {
            context.nextThread().executeThreadSafe([&]() {
                std::lock_guard<std::mutex> lock(uniqueThreadIdsMutex);
                uniqueThreadIds.insert(std::this_thread::get_id());
            });
        }

        // Wait for the loops to process the callbacks
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    ASSERT_EQ(uniqueThreadIds.size(), nThreads);
}

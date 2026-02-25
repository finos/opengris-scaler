#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <expected>
#include <mutex>
#include <set>
#include <thread>

#include "scaler/error/error.h"
#include "scaler/uv_ymq/address.h"
#include "scaler/uv_ymq/io_context.h"
#include "tests/cpp/ymq/common/testing.h"

class UVYMQTest: public ::testing::Test {};

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

TEST_F(UVYMQTest, IOContext)
{
    const size_t nTasks   = 10;
    const size_t nThreads = 4;

    std::set<std::thread::id> uniqueThreadIds {};
    std::mutex uniqueThreadIdsMutex {};

    {
        scaler::uv_ymq::IOContext context {nThreads};

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

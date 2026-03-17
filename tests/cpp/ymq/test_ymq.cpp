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

    address = scaler::ymq::Address::fromString("tcp://2001:db8::1:1211");
    ASSERT_TRUE(address.has_value());
    ASSERT_EQ(address->type(), scaler::ymq::Address::Type::TCP);

    address = scaler::ymq::Address::fromString("tcp://::1:8080");
    ASSERT_TRUE(address.has_value());
    ASSERT_EQ(address->type(), scaler::ymq::Address::Type::TCP);

    address = scaler::ymq::Address::fromString("ipc://some_ipc_socket_name");
    ASSERT_TRUE(address.has_value());
    ASSERT_EQ(address->type(), scaler::ymq::Address::Type::IPC);
    ASSERT_EQ(std::get<std::string>(address->value()), "some_ipc_socket_name");

    // Invalid addresses

    address = scaler::ymq::Address::fromString("http://127.0.0.1:8080");
    ASSERT_FALSE(address.has_value());

    address = scaler::ymq::Address::fromString("127.0.0.1:8080");
    ASSERT_FALSE(address.has_value());

    address = scaler::ymq::Address::fromString("tcp://127.0.0.1");
    ASSERT_FALSE(address.has_value());

    address = scaler::ymq::Address::fromString("");
    ASSERT_FALSE(address.has_value());
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

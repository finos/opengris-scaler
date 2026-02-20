#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>

#include "scaler/uv_ymq/internal/event_loop_thread.h"

class UVYMQInternalTest: public ::testing::Test {};

TEST_F(UVYMQInternalTest, EventLoopThread)
{
    const size_t nTasks = 3;

    std::atomic<int> nTimesCalled {0};

    {
        scaler::uv_ymq::internal::EventLoopThread thread {};

        for (size_t i = 0; i < nTasks; ++i) {
            thread.executeThreadSafe([&]() { ++nTimesCalled; });
        }

        // Wait for the loop to process the callbacks
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    ASSERT_EQ(nTimesCalled, nTasks);
}

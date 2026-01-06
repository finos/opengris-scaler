#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>

#include "scaler/uv_ymq/uv_loop_thread.h"

using namespace scaler::uv_ymq;

class UVYMQTest: public ::testing::Test {
protected:
};

TEST_F(UVYMQTest, UVLoopThread)
{
    const size_t N_TASKS = 3;

    std::atomic<int> nTimesCalled {0};

    {
        UVLoopThread thread {};

        for (size_t i = 0; i < N_TASKS; ++i) {
            thread.execute([&]() { ++nTimesCalled; });
        }

        // Wait for the loop to process the callbacks
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    ASSERT_EQ(nTimesCalled, N_TASKS);
}

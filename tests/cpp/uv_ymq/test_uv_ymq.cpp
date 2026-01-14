#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <mutex>
#include <set>
#include <thread>

#include "scaler/uv_ymq/event_loop_thread.h"
#include "scaler/uv_ymq/io_context.h"

using namespace scaler::uv_ymq;

class UVYMQTest: public ::testing::Test {
protected:
};

TEST_F(UVYMQTest, EventLoopThread)
{
    const size_t N_TASKS = 3;

    std::atomic<int> nTimesCalled {0};

    {
        EventLoopThread thread {};

        for (size_t i = 0; i < N_TASKS; ++i) {
            thread.execute([&]() { ++nTimesCalled; });
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
        IOContext context {N_THREADS};

        // Execute tasks on different threads in round-robin fashion
        for (size_t i = 0; i < N_TASKS; ++i) {
            context.nextThread().execute([&]() {
                std::lock_guard<std::mutex> lock(uniqueThreadIdsMutex);
                uniqueThreadIds.insert(std::this_thread::get_id());
            });
        }

        // Wait for the loops to process the callbacks
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    ASSERT_EQ(uniqueThreadIds.size(), N_THREADS);
}

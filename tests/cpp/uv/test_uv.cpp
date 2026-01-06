
#include <gtest/gtest.h>
#include <uv.h>

#include <chrono>
#include <string>
#include <type_traits>

#include "scaler/uv/async.h"
#include "scaler/uv/error.h"
#include "scaler/uv/loop.h"
#include "scaler/uv/signal.h"
#include "scaler/uv/timer.h"

using namespace scaler::uv;

class UVTest: public ::testing::Test {
protected:
    // Extract the value from std::expected or fail the test
    template <typename T>
    static T expectSuccess(std::expected<T, Error> result)
    {
        if (!result.has_value()) {
            throw std::runtime_error("Operation failed: " + result.error().message());
        }

        if constexpr (!std::is_void_v<T>) {
            return std::move(result.value());
        }
    }
};

TEST_F(UVTest, Async)
{
    Loop loop = expectSuccess(Loop::init());

    int nTimesCalled = 0;

    // Regular use-case
    {
        Async async = expectSuccess(Async::init(loop, [&]() { ++nTimesCalled; }));
        ASSERT_EQ(nTimesCalled, 0);

        loop.run(UV_RUN_NOWAIT);
        ASSERT_EQ(nTimesCalled, 0);

        expectSuccess(async.send());
        ASSERT_EQ(nTimesCalled, 0);

        loop.run(UV_RUN_NOWAIT);
        ASSERT_EQ(nTimesCalled, 1);
    }

    nTimesCalled = 0;

    // Destructing the Async object before running the loop should cancel the call.
    {
        Async async = expectSuccess(Async::init(loop, [&]() { ++nTimesCalled; }));
        expectSuccess(async.send());
    }

    int nActiveHandles = loop.run(UV_RUN_NOWAIT);

    ASSERT_EQ(nActiveHandles, 0);
    ASSERT_EQ(nTimesCalled, 0);
}

TEST_F(UVTest, Error)
{
    ASSERT_EQ(Error(UV_EAGAIN), Error(UV_EAGAIN));
    ASSERT_NE(Error(UV_EINTR), Error(UV_EAGAIN));

    ASSERT_EQ(Error(UV_EBUSY).name(), "EBUSY");
    ASSERT_EQ(Error(UV_EPIPE).message(), "broken pipe");
}

TEST_F(UVTest, Handle)
{
    Loop loop = expectSuccess(Loop::init());

    Handle<uv_timer_t, std::string> handle;
    uv_timer_init(&loop.native(), &handle.native());

    handle.setData("Some data");
    ASSERT_EQ(handle.data(), "Some data");

    handle.setData("Some other data");
    ASSERT_EQ(handle.data(), "Some other data");
}

TEST_F(UVTest, Loop)
{
    Loop loop = expectSuccess(Loop::init());

    // Loop::run()
    {
        int nActiveHandles = loop.run(UV_RUN_DEFAULT);
        ASSERT_EQ(nActiveHandles, 0);
    }

    // Loop::stop()
    {
        // Schedule a timer and a callback, with the callback stopping the loop before the timer can execute.

        int nTimesCalled = 0;

        Timer timer = expectSuccess(Timer::init(loop));
        Async async = expectSuccess(Async::init(loop, [&loop] { loop.stop(); }));

        expectSuccess(timer.start(std::chrono::milliseconds(1000), std::nullopt, [&]() { nTimesCalled++; }));
        expectSuccess(async.send());

        int nActiveHandles = loop.run(UV_RUN_DEFAULT);

        ASSERT_EQ(nActiveHandles, 1);
        ASSERT_EQ(nTimesCalled, 0);
    }
}

TEST_F(UVTest, Signal)
{
    constexpr int SIGNUM = SIGWINCH;

    Loop loop = expectSuccess(Loop::init());

    // Validates support for signals
    {
        Signal signal = expectSuccess(Signal::init(loop));
        expectSuccess(signal.start(SIGNUM, [&](int) {}));

        if (uv_kill(uv_os_getpid(), SIGNUM) == UV_ENOSYS) {
            GTEST_SKIP() << "uv_kill() is not supported on this platform";
            return;
        }
    }

    // Regular use-case
    {
        int nTimesCalled = 0;

        Signal signal = expectSuccess(Signal::init(loop));
        expectSuccess(signal.start(SIGNUM, [&](int) { nTimesCalled++; }));

        loop.run(UV_RUN_NOWAIT);
        ASSERT_EQ(nTimesCalled, 0);

        uv_kill(uv_os_getpid(), SIGNUM);
        uv_kill(uv_os_getpid(), SIGNUM);

        loop.run(UV_RUN_NOWAIT);
        ASSERT_EQ(nTimesCalled, 2);

        expectSuccess(signal.stop());
    }

    // One-shot signal
    {
        int nTimesCalled = 0;

        Signal signalOneShot = expectSuccess(Signal::init(loop));
        expectSuccess(signalOneShot.startOneshot(SIGNUM, [&](int) { nTimesCalled++; }));

        // Setup a 2nd "catch-all" signal handler, or else the 2nd uv_kill() will terminate the process because of the
        // default signal handler.
        Signal signal = expectSuccess(Signal::init(loop));
        expectSuccess(signal.start(SIGNUM, [&](int) {}));

        uv_kill(uv_os_getpid(), SIGNUM);
        uv_kill(uv_os_getpid(), SIGNUM);

        loop.run(UV_RUN_NOWAIT);

        ASSERT_EQ(nTimesCalled, 1);
    }
}

TEST_F(UVTest, Timer)
{
    constexpr std::chrono::milliseconds DELAY {50};

    Loop loop = expectSuccess(Loop::init());

    // Regular use-case
    {
        int nTimesCalled = 0;
        Timer timer      = expectSuccess(Timer::init(loop));

        expectSuccess(timer.start(DELAY, std::nullopt, [&]() { nTimesCalled++; }));

        // Timer should not be called immediately
        loop.run(UV_RUN_NOWAIT);
        ASSERT_EQ(nTimesCalled, 0);

        // Sleep and check timer was called
        std::this_thread::sleep_for(DELAY * 1.1);
        loop.run(UV_RUN_NOWAIT);
        ASSERT_GE(nTimesCalled, 1);
    }

    // Repeating and stopping timer
    {
        int nTimesCalled = 0;
        Timer timer      = expectSuccess(Timer::init(loop));

        expectSuccess(timer.start(std::chrono::milliseconds::zero(), DELAY, [&]() { nTimesCalled++; }));

        ASSERT_EQ(timer.getRepeat(), DELAY);

        // 0 second timeout should be called immediately.
        loop.run(UV_RUN_NOWAIT);
        ASSERT_EQ(nTimesCalled, 1);

        // Sleep and check timer was repeated
        std::this_thread::sleep_for(DELAY * 1.1);
        loop.run(UV_RUN_NOWAIT);
        ASSERT_EQ(nTimesCalled, 2);

        // Stop should prevent further executions
        expectSuccess(timer.stop());
        std::this_thread::sleep_for(DELAY * 1.1);
        loop.run(UV_RUN_NOWAIT);
        ASSERT_EQ(nTimesCalled, 2);
    }
}

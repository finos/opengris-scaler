#include <gtest/gtest.h>
#include <uv.h>

#include <chrono>
#include <string>
#include <vector>

#include "scaler/wrapper/uv/async.h"
#include "scaler/wrapper/uv/error.h"
#include "scaler/wrapper/uv/loop.h"
#include "scaler/wrapper/uv/request.h"
#include "scaler/wrapper/uv/signal.h"
#include "scaler/wrapper/uv/timer.h"
#include "tests/cpp/wrapper/uv/utility.h"

class UVTest: public ::testing::Test {
protected:
};

TEST_F(UVTest, Async)
{
    scaler::wrapper::uv::Loop loop = expectSuccess(scaler::wrapper::uv::Loop::init());

    int nTimesCalled = 0;

    // Regular use-case
    {
        scaler::wrapper::uv::Async async =
            expectSuccess(scaler::wrapper::uv::Async::init(loop, [&]() { ++nTimesCalled; }));
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
        scaler::wrapper::uv::Async async =
            expectSuccess(scaler::wrapper::uv::Async::init(loop, [&]() { ++nTimesCalled; }));
        expectSuccess(async.send());
    }

    int nActiveHandles = loop.run(UV_RUN_NOWAIT);

    ASSERT_EQ(nActiveHandles, 0);
    ASSERT_EQ(nTimesCalled, 0);
}

TEST_F(UVTest, Error)
{
    ASSERT_EQ(scaler::wrapper::uv::Error(UV_EAGAIN), scaler::wrapper::uv::Error(UV_EAGAIN));
    ASSERT_NE(scaler::wrapper::uv::Error(UV_EINTR), scaler::wrapper::uv::Error(UV_EAGAIN));

    ASSERT_EQ(scaler::wrapper::uv::Error(UV_EBUSY).name(), "EBUSY");
    ASSERT_EQ(scaler::wrapper::uv::Error(UV_EPIPE).message(), "broken pipe");
}

TEST_F(UVTest, Handle)
{
    scaler::wrapper::uv::Loop loop = expectSuccess(scaler::wrapper::uv::Loop::init());

    {
        scaler::wrapper::uv::Handle<uv_timer_t, std::string> handle;
        uv_timer_init(&loop.native(), &handle.native());

        handle.setData("Some data");
        ASSERT_EQ(handle.data(), "Some data");

        handle.setData("Some other data");
        ASSERT_EQ(handle.data(), "Some other data");
    }

    // Running the loop again should close the now destructed handle.
    {
        int nActiveHandles = loop.run(UV_RUN_NOWAIT);
        ASSERT_EQ(nActiveHandles, 0);
    }
}

TEST_F(UVTest, Loop)
{
    scaler::wrapper::uv::Loop loop = expectSuccess(scaler::wrapper::uv::Loop::init());

    // Loop::run()
    {
        int nActiveHandles = loop.run(UV_RUN_DEFAULT);
        ASSERT_EQ(nActiveHandles, 0);
    }

    // Loop::stop()
    {
        // Schedule a timer and a callback, with the callback stopping the loop before the timer can execute.

        int nTimesCalled = 0;

        scaler::wrapper::uv::Timer timer = expectSuccess(scaler::wrapper::uv::Timer::init(loop));
        scaler::wrapper::uv::Async async =
            expectSuccess(scaler::wrapper::uv::Async::init(loop, [&loop] { loop.stop(); }));

        expectSuccess(timer.start(std::chrono::milliseconds(1000), std::nullopt, [&]() { nTimesCalled++; }));
        expectSuccess(async.send());

        int nActiveHandles = loop.run(UV_RUN_DEFAULT);

        ASSERT_EQ(nActiveHandles, 1);
        ASSERT_EQ(nTimesCalled, 0);
    }
}

TEST_F(UVTest, Request)
{
    // Mock a write request

    int nTimesCalled = 0;

    scaler::wrapper::uv::WriteRequest request {[&](int status) { ++nTimesCalled; }};

    scaler::wrapper::uv::WriteRequest::onCallback(&request.native(), UV_EOVERFLOW);

    ASSERT_EQ(nTimesCalled, 1);
}

TEST_F(UVTest, Signal)
{
    constexpr int SIGNUM = SIGWINCH;

    scaler::wrapper::uv::Loop loop = expectSuccess(scaler::wrapper::uv::Loop::init());

    // Validates support for signals
    {
        scaler::wrapper::uv::Signal signal = expectSuccess(scaler::wrapper::uv::Signal::init(loop));
        expectSuccess(signal.start(SIGNUM, [&](int) {}));

        if (uv_kill(uv_os_getpid(), SIGNUM) == UV_ENOSYS) {
            GTEST_SKIP() << "uv_kill() is not supported on this platform";
            return;
        }
    }

    // Regular use-case
    {
        int nTimesCalled = 0;

        scaler::wrapper::uv::Signal signal = expectSuccess(scaler::wrapper::uv::Signal::init(loop));
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

        scaler::wrapper::uv::Signal signalOneShot = expectSuccess(scaler::wrapper::uv::Signal::init(loop));
        expectSuccess(signalOneShot.startOneshot(SIGNUM, [&](int) { nTimesCalled++; }));

        // Setup a 2nd "catch-all" signal handler, or else the 2nd uv_kill() will terminate the process because of the
        // default signal handler.
        scaler::wrapper::uv::Signal signal = expectSuccess(scaler::wrapper::uv::Signal::init(loop));
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

    scaler::wrapper::uv::Loop loop = expectSuccess(scaler::wrapper::uv::Loop::init());

    // Regular use-case
    {
        int nTimesCalled                 = 0;
        scaler::wrapper::uv::Timer timer = expectSuccess(scaler::wrapper::uv::Timer::init(loop));

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
        int nTimesCalled                 = 0;
        scaler::wrapper::uv::Timer timer = expectSuccess(scaler::wrapper::uv::Timer::init(loop));

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

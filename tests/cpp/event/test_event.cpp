#include <errno.h>
#include <gtest/gtest.h>

#include <string>

#include "scaler/event/async.h"
#include "scaler/event/error.h"
#include "scaler/event/loop.h"

using namespace scaler::event;

class EventTest: public ::testing::Test {
protected:
    // Extract the value from std::expected or fail the test
    template <typename T>
    static T expectSuccess(std::expected<T, Error> result)
    {
        if (!result.has_value()) {
            throw std::runtime_error("Operation failed: " + result.error().message());
        }
        return std::move(result.value());
    }
};

TEST_F(EventTest, Async)
{
    Loop loop = expectSuccess(Loop::init());

    int nTimesCalled = 0;

    // Regular use-case
    {
        Async async = expectSuccess(Async::init(loop, [&]() { ++nTimesCalled; }));
        ASSERT_EQ(nTimesCalled, 0);

        loop.run(UV_RUN_NOWAIT);
        ASSERT_EQ(nTimesCalled, 0);

        async.send();
        ASSERT_EQ(nTimesCalled, 0);

        loop.run(UV_RUN_NOWAIT);
        ASSERT_EQ(nTimesCalled, 1);
    }

    nTimesCalled = 0;

    // Destructing the Async object before running the loop should cancel the call.
    {
        Async async = expectSuccess(Async::init(loop, [&]() { ++nTimesCalled; }));
        async.send();
    }

    int nActiveHandles = loop.run(UV_RUN_NOWAIT);

    ASSERT_EQ(nActiveHandles, 0);
    ASSERT_EQ(nTimesCalled, 0);
}

TEST_F(EventTest, Error)
{
    ASSERT_EQ(Error(UV_EAGAIN), Error(UV_EAGAIN));
    ASSERT_NE(Error(UV_EINTR), Error(UV_EAGAIN));

    ASSERT_EQ(Error(UV_EBUSY).name(), "EBUSY");
    ASSERT_EQ(Error(UV_EPIPE).message(), "broken pipe");

    ASSERT_EQ(Error::fromSysError(EACCES), Error(UV_EACCES));
}

TEST_F(EventTest, Handle)
{
    Loop loop = expectSuccess(Loop::init());

    Handle<uv_timer_t, std::string> handle;
    uv_timer_init(&loop.native(), &handle.native());

    handle.setData("Some data");
    ASSERT_EQ(handle.data(), "Some data");

    handle.setData("Some other data");
    ASSERT_EQ(handle.data(), "Some other data");
}

TEST_F(EventTest, Loop)
{
    Loop loop = expectSuccess(Loop::init());

    // Loop::run()
    {
        int nActiveHandles = loop.run(UV_RUN_DEFAULT);
        ASSERT_EQ(nActiveHandles, 0);
    }

    // Loop::stop()
    {
        int nTimesCalled = 0;

        // Schedule a timer and a callback, with the callback stopping the loop before the timer can execute.

        // Timer timer = expectSuccess(Timer::init(loop, [&nTimesCalled] { ++nTimesCalled; }));
        Async async = expectSuccess(Async::init(loop, [&loop] { loop.stop(); }));

        // timer.start();
        async.send();

        int nActiveHandles = loop.run(UV_RUN_DEFAULT);

        ASSERT_EQ(nActiveHandles, 1);
        ASSERT_EQ(nTimesCalled, 0);
    }
}

#include <errno.h>
#include <gtest/gtest.h>

#include "scaler/event/error.h"
#include "scaler/event/loop.h"

using namespace scaler::event;

class EventTest: public ::testing::Test {};

TEST_F(EventTest, Error)
{
    ASSERT_EQ(Error::EAGAIN, Error::EAGAIN);
    ASSERT_NE(Error::EINTR, Error::EAGAIN);

    ASSERT_EQ(Error::EBUSY.name(), "EBUSY");
    ASSERT_EQ(Error::EPIPE.name(), "broken pipe");

    ASSERT_EQ(Error::fromSysError(EACCES), Error::EACCES);
}

TEST_F(EventTest, Loop)
{
    Loop loop();
    int nActiveHandles = loop->run(UV_RUN_DEFAULT);

    ASSERT_EQ(nActiveHandles, 0);
}

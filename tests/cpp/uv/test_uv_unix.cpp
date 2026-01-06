#include <errno.h>
#include <gtest/gtest.h>
#include <uv.h>

#include "scaler/uv/error.h"

using namespace scaler::uv;

class UVTestUnix: public ::testing::Test {};

TEST_F(UVTestUnix, Error)
{
    ASSERT_EQ(Error::fromSysError(EEXIST), Error(UV_EEXIST));
}

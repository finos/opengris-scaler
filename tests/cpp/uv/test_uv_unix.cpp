#include <errno.h>
#include <gtest/gtest.h>
#include <uv.h>

#include "scaler/uv/error.h"

class UVTestUnix: public ::testing::Test {};

TEST_F(UVTestUnix, Error)
{
    ASSERT_EQ(scaler::uv::Error::fromSysError(EEXIST), scaler::uv::Error(UV_EEXIST));
}

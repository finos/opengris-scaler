#include <errno.h>
#include <gtest/gtest.h>
#include <uv.h>

#include "scaler/wrapper/uv/error.h"

class UVTestUnix: public ::testing::Test {};

TEST_F(UVTestUnix, Error)
{
    ASSERT_EQ(scaler::wrapper::uv::Error::fromSysError(EEXIST), scaler::wrapper::uv::Error(UV_EEXIST));
}

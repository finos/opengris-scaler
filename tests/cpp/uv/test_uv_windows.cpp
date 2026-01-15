#include <gtest/gtest.h>
#include <uv.h>
#include <windows.h>
#include <winerror.h>

#include "scaler/uv/error.h"

class UVTestWindows: public ::testing::Test {};

TEST_F(UVTestWindows, Error)
{
    ASSERT_EQ(scaler::uv::Error::fromSysError(ERROR_FILE_EXISTS), scaler::uv::Error(UV_EEXIST));
}

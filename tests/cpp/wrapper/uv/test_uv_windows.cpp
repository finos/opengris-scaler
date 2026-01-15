#include <gtest/gtest.h>
#include <uv.h>
#include <windows.h>
#include <winerror.h>

#include "scaler/wrapper/uv/error.h"

class UVTestWindows: public ::testing::Test {};

TEST_F(UVTestWindows, Error)
{
    ASSERT_EQ(scaler::wrapper::uv::Error::fromSysError(ERROR_FILE_EXISTS), scaler::wrapper::uv::Error(UV_EEXIST));
}

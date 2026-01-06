#include <gtest/gtest.h>
#include <uv.h>
#include <windows.h>
#include <winerror.h>

#include "scaler/uv/error.h"

using namespace scaler::uv;

class UVTestWindows: public ::testing::Test {};

TEST_F(UVTestWindows, Error)
{
    ASSERT_EQ(Error::fromSysError(ERROR_FILE_EXISTS), Error(UV_EEXIST));
}

#include <gtest/gtest.h>

#include <cassert>
#include <cstdlib>

#include "scaler/io/ymq/examples/common.h"
#include "scaler/io/ymq/io_context.h"
#include "tests/cc_ymq/bad_header.h"
#include "tests/cc_ymq/basic.h"
#include "tests/cc_ymq/common.h"
#include "tests/cc_ymq/empty_message.h"
#include "tests/cc_ymq/incomplete_identity.h"
#include "tests/cc_ymq/reconnect.h"
#include "tests/cc_ymq/slow.h"

using namespace scaler::ymq;
using namespace std::chrono_literals;

TEST(CcYmqTestSuite, TestBasicDelay)
{
    auto result = test(10, [] { return basic_client_main(3); }, basic_server_main);
    EXPECT_EQ(result, TestResult::Success);
}

TEST(CcYmqTestSuite, TestBasicNoDelay)
{
    auto result = test(10, [] { return basic_client_main(0); }, basic_server_main);

    // TODO: this should pass
    EXPECT_EQ(result, TestResult::Failure);
}

TEST(CcYmqTestSuite, TestSlowNetwork)
{
    auto result = test(20, slow_client_main, slow_server_main);
    EXPECT_EQ(result, TestResult::Success);
}

TEST(CcYmqTestSuite, TestIncompleteIdentity)
{
    auto result = test(20, incomplete_identity_client_main, incomplete_identity_server_main);
    EXPECT_EQ(result, TestResult::Success);
}

TEST(CcYmqTestSuite, TestBadHeader)
{
    auto result = test(20, bad_header_client_main, bad_header_server_main);

    // TODO: this should pass
    EXPECT_EQ(result, TestResult::Failure);
}

TEST(CcYmqTestSuite, TestEmptyMessage)
{
    auto result = test(20, empty_message_client_main, empty_message_server_main);
    EXPECT_EQ(result, TestResult::Success);
}

#include <gtest/gtest.h>

#include <chrono>
#include <cstdio>
#include <fstream>
#include <limits>
#include <string>
#include <thread>
#include <vector>

#include "scaler/io/ymq/logging.h"

// Test fixture for direct unit testing of the log() function's formatting
class LoggingUnitTest: public ::testing::Test {
protected:
    const std::string log_filename = "unit_test_log_output.txt";

    void SetUp() override { std::remove(log_filename.c_str()); }

    void TearDown() override { std::remove(log_filename.c_str()); }

    std::string readLogFile()
    {
        std::ifstream file(log_filename);
        if (!file.is_open())
            return "";
        std::stringstream buffer;
        buffer << file.rdbuf();
        std::string content = buffer.str();
        if (!content.empty() && content.back() == '\n') {
            content.pop_back();
        }
        return content;
    }
};

TEST_F(LoggingUnitTest, TestLogsEmptyMessage)
{
    scaler::ymq::Logger logger("%(levelname)s: %(message)s", log_filename, scaler::ymq::Logger::LoggingLevel::info);
    logger.log(scaler::ymq::Logger::LoggingLevel::info, "");
    EXPECT_EQ(readLogFile(), "INFO: ");
}

TEST_F(LoggingUnitTest, TestLogsPercentEscape)
{
    scaler::ymq::Logger logger(
        "This is a test %% with percent signs", log_filename, scaler::ymq::Logger::LoggingLevel::debug);
    logger.log(scaler::ymq::Logger::LoggingLevel::debug);
    EXPECT_EQ(readLogFile(), "This is a test % with percent signs");
}

TEST_F(LoggingUnitTest, TestLogsUnknownToken)
{
    scaler::ymq::Logger logger(
        "Token %(invalid)s should not be replaced.", log_filename, scaler::ymq::Logger::LoggingLevel::error);
    logger.log(scaler::ymq::Logger::LoggingLevel::error);
    EXPECT_EQ(readLogFile(), "Token %(invalid)s should not be replaced.");
}

TEST_F(LoggingUnitTest, TestLogsMultipleArgumentTypes)
{
    scaler::ymq::Logger logger(
        "%(levelname)s: Error on line %(message)s", log_filename, scaler::ymq::Logger::LoggingLevel::critical);
    int line_number = 42;
    double value    = 3.14;
    logger.log(scaler::ymq::Logger::LoggingLevel::critical, line_number, " with value ", value);
    EXPECT_EQ(readLogFile(), "CTIC: Error on line 42 with value 3.14");
}

TEST_F(LoggingUnitTest, TestLogsLargeNumbers)
{
    scaler::ymq::Logger logger("%(message)s", log_filename, scaler::ymq::Logger::LoggingLevel::info);
    long long large_number = std::numeric_limits<long long>::max();
    logger.log(scaler::ymq::Logger::LoggingLevel::info, "Large number: ", large_number);

    std::string expected = "Large number: " + std::to_string(large_number);
    EXPECT_EQ(readLogFile(), expected);
}

TEST_F(LoggingUnitTest, TestLogsMalformedToken)
{
    scaler::ymq::Logger logger(
        "Malformed token %(message should be literal", log_filename, scaler::ymq::Logger::LoggingLevel::info);
    logger.log(scaler::ymq::Logger::LoggingLevel::info);
    EXPECT_EQ(readLogFile(), "Malformed token %(message should be literal");
}

TEST_F(LoggingUnitTest, TestLogsTrailingPercent)
{
    scaler::ymq::Logger logger(
        "Message with a trailing percent%", log_filename, scaler::ymq::Logger::LoggingLevel::info);
    logger.log(scaler::ymq::Logger::LoggingLevel::info);
    EXPECT_EQ(readLogFile(), "Message with a trailing percent%");
}

TEST_F(LoggingUnitTest, TestLogsMismatchedArguments)
{
    scaler::ymq::Logger logger(
        "[%(levelname)s] No message token here", log_filename, scaler::ymq::Logger::LoggingLevel::info);
    logger.log(scaler::ymq::Logger::LoggingLevel::info, "this part is ignored", 123);
    EXPECT_EQ(readLogFile(), "[INFO] No message token here");
}
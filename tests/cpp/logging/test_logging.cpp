#include <gtest/gtest.h>

#include <cstdio>
#include <fstream>
#include <limits>
#include <string>
#include <vector>

#include "scaler/logging/logging.h"

// Test fixture for direct unit testing of the log() function's formatting
class LoggingUnitTest: public ::testing::Test {
protected:
    // Use a vector to hold log filenames for cleanup
    std::vector<std::string> log_filenames;
    const std::string curr_log_filename = "unit_test_log_output.txt";

    void SetUp() override
    {
        // Add the main log file for cleanup by default
        log_filenames.push_back(curr_log_filename);
        cleanupLogFiles();
    }

    void TearDown() override { cleanupLogFiles(); }

    void cleanupLogFiles()
    {
        for (const auto& filename: log_filenames) {
            std::remove(filename.c_str());
        }
    }

    std::string readLogFile(const std::string& filename)
    {
        std::ifstream file(filename);
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
    scaler::ymq::Logger logger(
        "%(levelname)s: %(message)s", {curr_log_filename}, scaler::ymq::Logger::LoggingLevel::info);
    logger.log(scaler::ymq::Logger::LoggingLevel::info, "");
    EXPECT_EQ(readLogFile(curr_log_filename), "INFO: ");
}

TEST_F(LoggingUnitTest, TestLogsPercentEscape)
{
    scaler::ymq::Logger logger(
        "This is a test %% with percent signs", {curr_log_filename}, scaler::ymq::Logger::LoggingLevel::debug);
    logger.log(scaler::ymq::Logger::LoggingLevel::debug);
    EXPECT_EQ(readLogFile(curr_log_filename), "This is a test % with percent signs");
}

TEST_F(LoggingUnitTest, TestLogsUnknownToken)
{
    scaler::ymq::Logger logger(
        "Token %(invalid)s should not be replaced.", {curr_log_filename}, scaler::ymq::Logger::LoggingLevel::error);
    logger.log(scaler::ymq::Logger::LoggingLevel::error);
    EXPECT_EQ(readLogFile(curr_log_filename), "Token %(invalid)s should not be replaced.");
}

TEST_F(LoggingUnitTest, TestLogsMultipleArgumentTypes)
{
    scaler::ymq::Logger logger(
        "%(levelname)s: Error on line %(message)s", {curr_log_filename}, scaler::ymq::Logger::LoggingLevel::critical);
    int line_number = 42;
    double value    = 3.14;
    logger.log(scaler::ymq::Logger::LoggingLevel::critical, line_number, " with value ", value);
    EXPECT_EQ(readLogFile(curr_log_filename), "CTIC: Error on line 42 with value 3.14");
}

TEST_F(LoggingUnitTest, TestLogsLargeNumbers)
{
    scaler::ymq::Logger logger("%(message)s", {curr_log_filename}, scaler::ymq::Logger::LoggingLevel::info);
    long long large_number = std::numeric_limits<long long>::max();
    logger.log(scaler::ymq::Logger::LoggingLevel::info, "Large number: ", large_number);

    std::string expected = "Large number: " + std::to_string(large_number);
    EXPECT_EQ(readLogFile(curr_log_filename), expected);
}

TEST_F(LoggingUnitTest, TestLogsMalformedToken)
{
    scaler::ymq::Logger logger(
        "Malformed token %(message should be literal", {curr_log_filename}, scaler::ymq::Logger::LoggingLevel::info);
    logger.log(scaler::ymq::Logger::LoggingLevel::info);
    EXPECT_EQ(readLogFile(curr_log_filename), "Malformed token %(message should be literal");
}

TEST_F(LoggingUnitTest, TestLogsTrailingPercent)
{
    scaler::ymq::Logger logger(
        "Message with a trailing percent%", {curr_log_filename}, scaler::ymq::Logger::LoggingLevel::info);
    logger.log(scaler::ymq::Logger::LoggingLevel::info);
    EXPECT_EQ(readLogFile(curr_log_filename), "Message with a trailing percent%");
}

TEST_F(LoggingUnitTest, TestLogsMismatchedArguments)
{
    scaler::ymq::Logger logger(
        "[%(levelname)s] No message token here", {curr_log_filename}, scaler::ymq::Logger::LoggingLevel::info);
    logger.log(scaler::ymq::Logger::LoggingLevel::info, "this part is ignored", 123);
    EXPECT_EQ(readLogFile(curr_log_filename), "[INFO] No message token here");
}

// New test case to verify logging to multiple files
TEST_F(LoggingUnitTest, TestLogsToMultipleFiles)
{
    const std::string log_file1 = "multi_log_output1.txt";
    const std::string log_file2 = "multi_log_output2.txt";

    // Add these files to the list for automatic cleanup
    log_filenames.push_back(log_file1);
    log_filenames.push_back(log_file2);

    std::vector<std::string> paths = {log_file1, log_file2};

    scaler::ymq::Logger logger("%(levelname)s - %(message)s", paths, scaler::ymq::Logger::LoggingLevel::warning);

    logger.log(scaler::ymq::Logger::LoggingLevel::warning, "This message should appear in both files.");

    std::string expected_output = "WARN - This message should appear in both files.";

    EXPECT_EQ(readLogFile(log_file1), expected_output);
    EXPECT_EQ(readLogFile(log_file2), expected_output);
}

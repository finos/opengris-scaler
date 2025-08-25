#include <gtest/gtest.h>
#include <fstream>
#include <cstdio>
#include <string>
#include <vector>
#include <thread>
#include <chrono>
#include <limits>

#include "scaler/io/ymq/logging.h"
#include "scaler/object_storage/object_storage_server.h"

// using boost::asio::awaitable;
using boost::asio::buffer;
using boost::asio::ip::tcp;

// Helper function to find an available TCP port.
int getAvailableTCPPort() {
    boost::asio::io_context ioContext;
    tcp::acceptor acceptor(ioContext);
    boost::system::error_code ec;
    acceptor.open(tcp::v4(), ec);
    acceptor.bind(tcp::endpoint(tcp::v4(), 0), ec);
    int port = acceptor.local_endpoint().port();
    acceptor.close();
    return port;
}

// Test fixture for direct unit testing of the log() function's formatting
class LoggingUnitTest : public ::testing::Test {
protected:
    const std::string log_filename = "unit_test_log_output.txt";
    scaler::ymq::LoggingLevel original_log_level_;

    void SetUp() override {
        std::remove(log_filename.c_str());
        original_log_level_ = scaler::ymq::LOGGING_LEVEL;
    }

    void TearDown() override {
        std::remove(log_filename.c_str());
        scaler::ymq::LOGGING_LEVEL = original_log_level_;
    }

    std::string readLogFile() {
        std::ifstream file(log_filename);
        if (!file.is_open()) return "";
        std::stringstream buffer;
        buffer << file.rdbuf();
        std::string content = buffer.str();
        if (!content.empty() && content.back() == '\n') {
            content.pop_back();
        }
        return content;
    }
};

TEST_F(LoggingUnitTest, TestLogsEmptyMessage) {
    scaler::ymq::LOGGING_LEVEL = scaler::ymq::LoggingLevel::info;
    scaler::ymq::log(scaler::ymq::LoggingLevel::info, "%(levelname)s: %(message)s", log_filename, "");
    EXPECT_EQ(readLogFile(), "INFO: ");
}

TEST_F(LoggingUnitTest, TestLogsPercentEscape) {
    scaler::ymq::LOGGING_LEVEL = scaler::ymq::LoggingLevel::debug;
    scaler::ymq::log(scaler::ymq::LoggingLevel::debug, "This is a test %% with percent signs", log_filename);
    EXPECT_EQ(readLogFile(), "This is a test % with percent signs");
}

TEST_F(LoggingUnitTest, TestLogsUnknownToken) {
    scaler::ymq::LOGGING_LEVEL = scaler::ymq::LoggingLevel::error;
    scaler::ymq::log(scaler::ymq::LoggingLevel::error, "Token %(invalid)s should not be replaced.", log_filename);
    EXPECT_EQ(readLogFile(), "Token %(invalid)s should not be replaced.");
}

TEST_F(LoggingUnitTest, TestLogsMultipleArgumentTypes) {
    scaler::ymq::LOGGING_LEVEL = scaler::ymq::LoggingLevel::critical;
    int line_number = 42;
    double value = 3.14;
    scaler::ymq::log(scaler::ymq::LoggingLevel::critical, 
                     "%(levelname)s: Error on line %(message)s", 
                     log_filename, 
                     line_number, " with value ", value);
    EXPECT_EQ(readLogFile(), "CTIC: Error on line 42 with value 3.14");
}

TEST_F(LoggingUnitTest, TestLogsLargeNumbers) {
    scaler::ymq::LOGGING_LEVEL = scaler::ymq::LoggingLevel::info;
    long long large_number = std::numeric_limits<long long>::max();
    scaler::ymq::log(scaler::ymq::LoggingLevel::info, 
                     "%(message)s", 
                     log_filename, 
                     "Large number: ", large_number);
    
    std::string expected = "Large number: " + std::to_string(large_number);
    EXPECT_EQ(readLogFile(), expected);
}

TEST_F(LoggingUnitTest, TestLogsMalformedToken) {
    scaler::ymq::LOGGING_LEVEL = scaler::ymq::LoggingLevel::info;
    scaler::ymq::log(scaler::ymq::LoggingLevel::info, "Malformed token %(message should be literal", log_filename);
    EXPECT_EQ(readLogFile(), "Malformed token %(message should be literal");
}

TEST_F(LoggingUnitTest, TestLogsTrailingPercent) {
    scaler::ymq::LOGGING_LEVEL = scaler::ymq::LoggingLevel::info;
    scaler::ymq::log(scaler::ymq::LoggingLevel::info, "Message with a trailing percent%", log_filename);
    EXPECT_EQ(readLogFile(), "Message with a trailing percent%");
}

TEST_F(LoggingUnitTest, TestLogsMismatchedArguments) {
    scaler::ymq::LOGGING_LEVEL = scaler::ymq::LoggingLevel::info;
    scaler::ymq::log(scaler::ymq::LoggingLevel::info, 
                     "[%(levelname)s] No message token here", 
                     log_filename, 
                     "this part is ignored", 123);
    EXPECT_EQ(readLogFile(), "[INFO] No message token here");
}

class ObjectStorageLoggingTest : public ::testing::Test {
protected:
    static constexpr std::string SERVER_HOST = "127.0.0.1";
    const std::string log_filename = "server_integration_log.txt";

    std::unique_ptr<scaler::object_storage::ObjectStorageServer> server;
    std::string serverPort;
    std::thread serverThread;
    boost::asio::io_context ioContext;

    class SimpleClient {
    public:
        SimpleClient(boost::asio::io_context& ioContext, const std::string& host, const std::string& port)
            : socket(ioContext) {
            tcp::resolver resolver(ioContext);
            boost::asio::connect(socket, resolver.resolve(host, port));
        }
        ~SimpleClient() {
            boost::system::error_code ec;
            socket.shutdown(tcp::socket::shutdown_both, ec);
            socket.close(ec);
        }
    private:
        tcp::socket socket;
    };

    void SetUp() override {
        std::remove(log_filename.c_str());
        server = std::make_unique<scaler::object_storage::ObjectStorageServer>();
        serverPort = std::to_string(getAvailableTCPPort());
        serverThread = std::thread([this] {
            server->run(SERVER_HOST, serverPort, "INFO", "%(levelname)s: %(message)s", log_filename);
        });
        server->waitUntilReady();
    }

    void TearDown() override {
        server->shutdown();
        if (serverThread.joinable()) {
            serverThread.join();
        }
        server.reset();
        std::remove(log_filename.c_str());
    }

    std::string readLogFile() {
        std::ifstream file(log_filename);
        if (!file.is_open()) return "";
        std::stringstream buffer;
        buffer << file.rdbuf();
        return buffer.str();
    }
};

TEST_F(ObjectStorageLoggingTest, TestServerLogsDisconnectionToFile) {
    {
        SimpleClient client(ioContext, SERVER_HOST, serverPort);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    std::string log_content = readLogFile();
    EXPECT_NE(log_content.find("INFO: Remote end closed, nothing to read."), std::string::npos);
}

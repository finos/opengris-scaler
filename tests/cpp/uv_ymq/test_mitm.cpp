// MITM (man-in-the-middle) tests from YMQ.
//
// The men in the middle are implemented using Python and are found in py_mitm/.
// In that directory, `main.py` is the entrypoint and framework for all the MITM, and the individual MITM
// implementations are found in their respective files
#include <gtest/gtest.h>

#ifdef __linux__
#include <fcntl.h>
#include <netinet/ip.h>
#include <semaphore.h>
#include <sys/mman.h>
#endif  // __linux__

#ifdef _WIN32
#define NOMINMAX
#include <windows.h>
#endif  // _WIN32

#include <chrono>
#include <format>
#include <string>

#include "scaler/uv_ymq/future/connector_socket.h"
#include "scaler/uv_ymq/io_context.h"
#include "scaler/uv_ymq/sync/binder_socket.h"
#include "scaler/ymq/bytes.h"
#include "tests/cpp/ymq/common/testing.h"
#include "tests/cpp/ymq/common/utils.h"

// Helper client/server functions defined in test_sockets.cpp
TestResult basicClientYmq(std::string address);
TestResult basicServerYmq(std::string address);

class UVYMQMitmTest: public ::testing::Test {};

// Ensures the MITM scripts are initialized (only once) before any MITM tests run.
class MITMEnvironment: public ::testing::Environment {
public:
    void SetUp() override
    {
        ensurePythonInitialized();

#ifdef _WIN32
        // initialize winsock
        WSADATA wsaData = {};
        int iResult     = WSAStartup(MAKEWORD(2, 2), &wsaData);
        if (iResult != 0) {
            std::cerr << "WSAStartup failed: " << iResult << "\n";
        }
#endif  // _WIN32
    }

    void TearDown() override
    {
#ifdef _WIN32
        WSACleanup();
#endif  // _WIN32

        maybeFinalizePython();
    }
};

static ::testing::Environment* const mitmEnvironment = ::testing::AddGlobalTestEnvironment(new MITMEnvironment);

TestResult reconnectServerMain(std::string address)
{
    scaler::uv_ymq::IOContext context {};

    scaler::uv_ymq::sync::BinderSocket socket {context, "server"};
    auto bindResult = socket.bindTo(address);
    RETURN_FAILURE_IF_FALSE(bindResult.has_value());

    auto result = socket.recvMessage();

    RETURN_FAILURE_IF_FALSE(result.has_value());
    RETURN_FAILURE_IF_FALSE(result->payload.as_string() == "sync");

    auto error = socket.sendMessage("client", scaler::ymq::Bytes {"acknowledge"});
    RETURN_FAILURE_IF_FALSE(error.has_value());

    return TestResult::Success;
}

TestResult reconnectClientMain(std::string address)
{
    constexpr int retryTimes = 10;
    constexpr std::chrono::seconds retryDelay {1};

    scaler::uv_ymq::IOContext context {};

    auto socketResult = scaler::uv_ymq::future::ConnectorSocket::init(context, "client", address);
    RETURN_FAILURE_IF_FALSE(socketResult.has_value());

    auto socket = std::move(socketResult.value());

    // create the recv future in advance, this remains active between reconnects
    auto future = socket.recvMessage();

    // send "sync" and wait for "acknowledge" in a loop
    // the mitm will send a RST after the first "sync"
    // the "sync" message will be lost, but uv_ymq should automatically reconnect
    // therefore the next "sync" message should succeed
    for (size_t i = 0; i < retryTimes; i++) {
        auto sendFuture = socket.sendMessage(scaler::ymq::Bytes {"sync"});
        RETURN_FAILURE_IF_FALSE(sendFuture.get().has_value());

        auto result = future.wait_for(retryDelay);
        if (result == std::future_status::ready) {
            auto msg = future.get();
            RETURN_FAILURE_IF_FALSE(msg.has_value());
            RETURN_FAILURE_IF_FALSE(msg->payload.as_string() == "acknowledge");
            return TestResult::Success;
        } else if (result == std::future_status::timeout) {
            // timeout, try again
            continue;
        } else {
            std::cerr << "future status error\n";
            return TestResult::Failure;
        }
    }

    std::cerr << "failed to reconnect after " << retryTimes << " attempts\n";
    return TestResult::Failure;
}

// this is the no-op/passthrough man in the middle test
// for this test case we use uv_ymq on both the client side and the server side
// the client connects to the mitm, and the mitm connects to the server
// when the mitm receives packets from the client, it forwards it to the server without changing it
// and similarly when it receives packets from the server, it forwards them to the client
//
// the mitm is implemented in Python. we pass the name of the test case, which corresponds to the Python filename,
// and a list of arguments, which are: mitm ip, mitm port, remote ip, remote port
// this defines the address of the mitm, and the addresses that can connect to it
// for more, see the python mitm files
TEST_F(UVYMQMitmTest, Passthrough)
{
#ifdef __linux__
    auto mitm_ip   = "192.0.2.4";
    auto remote_ip = "192.0.2.3";
#else
    auto mitm_ip   = "127.0.0.1";
    auto remote_ip = "127.0.0.1";
#endif
    auto mitm_port   = randomPort();
    auto remote_port = 23571;

    // the Python program must be the first and only the first function passed to test()
    // we must also pass `true` as the third argument to ensure that Python is fully started
    // before beginning the test
    auto result = test(
        20,
        {[=] { return runMitm("passthrough", mitm_ip, mitm_port, remote_ip, remote_port); },
         [=] { return basicServerYmq(std::format("tcp://{}:{}", remote_ip, remote_port)); },
         [=] { return basicClientYmq(std::format("tcp://{}:{}", mitm_ip, mitm_port)); }},
        true);

    EXPECT_EQ(result, TestResult::Success);
}

// this is the same as the above, but both the client and server use raw sockets
TEST_F(UVYMQMitmTest, PassthroughRaw)
{
#ifdef __linux__
    auto mitm_ip   = "192.0.2.4";
    auto remote_ip = "192.0.2.3";
#else
    auto mitm_ip   = "127.0.0.1";
    auto remote_ip = "127.0.0.1";
#endif
    auto mitm_port   = randomPort();
    auto remote_port = 23574;

    // the Python program must be the first and only the first function passed to test()
    // we must also pass `true` as the third argument to ensure that Python is fully started
    // before beginning the test
    auto result = test(
        20,
        {[=] { return runMitm("passthrough", mitm_ip, mitm_port, remote_ip, remote_port); },
         [=] { return basicServerYmq(std::format("tcp://{}:{}", remote_ip, remote_port)); },
         [=] { return basicClientYmq(std::format("tcp://{}:{}", mitm_ip, mitm_port)); }},
        true);
    EXPECT_EQ(result, TestResult::Success);
}

// this test uses the mitm to test the reconnect logic of uv_ymq by sending RST packets
#ifdef _WIN32
// See https://github.com/finos/opengris-scaler/issues/576
TEST_F(UVYMQMitmTest, DISABLED_Reconnect)
#else
TEST_F(UVYMQMitmTest, Reconnect)
#endif
{
#ifdef __linux__
    auto mitm_ip   = "192.0.2.4";
    auto remote_ip = "192.0.2.3";
#else
    auto mitm_ip   = "127.0.0.1";
    auto remote_ip = "127.0.0.1";
#endif
    auto mitm_port   = randomPort();
    auto remote_port = 23572;

    auto result = test(
        30,
        {[=] { return runMitm("send_rst_to_client", mitm_ip, mitm_port, remote_ip, remote_port); },
         [=] { return reconnectServerMain(std::format("tcp://{}:{}", remote_ip, remote_port)); },
         [=] { return reconnectClientMain(std::format("tcp://{}:{}", mitm_ip, mitm_port)); }},
        true);

    EXPECT_EQ(result, TestResult::Success);
}

// in this test, the mitm drops a random % of packets arriving from the client and server
TEST_F(UVYMQMitmTest, RandomlyDropPackets)
{
#ifdef __linux__
    auto mitm_ip   = "192.0.2.4";
    auto remote_ip = "192.0.2.3";
#else
    auto mitm_ip   = "127.0.0.1";
    auto remote_ip = "127.0.0.1";
#endif
    auto mitm_port   = randomPort();
    auto remote_port = 23573;

    auto result = test(
        180,
        {[=] { return runMitm("randomly_drop_packets", mitm_ip, mitm_port, remote_ip, remote_port, {"0.3"}); },
         [=] { return basicServerYmq(std::format("tcp://{}:{}", remote_ip, remote_port)); },
         [=] { return basicClientYmq(std::format("tcp://{}:{}", mitm_ip, mitm_port)); }},
        true);

    EXPECT_EQ(result, TestResult::Success);
}

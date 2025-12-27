// this file contains the tests for the C++ interface of YMQ
// each test case is comprised of at least one client and one server, and possibly a middleman
// the clients and servers used in these tests are defined in the first part of this file
//
// the men in the middle (mitm) are implemented using Python and are found in py_mitm/
// in that directory, `main.py` is the entrypoint and framework for all the mitm,
// and the individual mitm implementations are found in their respective files
//
// the test cases are at the bottom of this file, after the clients and servers
// the documentation for each case is found on the TEST() definition
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
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <format>
#include <future>
#include <limits>
#include <string>
#include <thread>

#include "scaler/error/error.h"
#include "scaler/ymq/bytes.h"
#include "scaler/ymq/io_context.h"
#include "scaler/ymq/simple_interface.h"
#include "tests/cpp/ymq/common/testing.h"
#include "tests/cpp/ymq/common/utils.h"
#include "tests/cpp/ymq/net/socket_utils.h"

using namespace scaler::ymq;
using namespace std::chrono_literals;

// a test suite that's parameterized by transport protocol (e.g. "tcp", "ipc")
class CcYmqTestSuiteParametrized: public ::testing::TestWithParam<std::string> {
protected:
    std::string GetAddress(int port)
    {
        const std::string& transport = GetParam();
        if (transport == "tcp") {
            return std::format("tcp://127.0.0.1:{}", port);
        }
#ifdef __linux__
        if (transport == "ipc") {
            // using a unique path for each test based on port
            const char* runner_temp = std::getenv("RUNNER_TEMP");
            if (runner_temp) {
                return std::format("ipc://{}/ymq-test-{}.ipc", runner_temp, port);
            }
            return std::format("ipc:///tmp/ymq-test-{}.ipc", port);
        }
#endif
        // Gtest should not select this for unsupported platforms, but as a fallback,
        // return something that will cause tests to fail clearly.
        return "invalid-transport";
    }
};

// --------------------
//  clients and servers
// --------------------
TestResult basic_server_ymq(std::string address)
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Binder, "server");
    syncBindSocket(socket, address);
    auto result = syncRecvMessage(socket);

    RETURN_FAILURE_IF_FALSE(result.has_value());
    RETURN_FAILURE_IF_FALSE(result->payload.as_string() == "yi er san si wu liu");

    context.removeIOSocket(socket);

    return TestResult::Success;
}

TestResult basic_client_ymq(std::string address)
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Connector, "client");
    syncConnectSocket(socket, address);
    auto result = syncSendMessage(socket, {.address = Bytes("server"), .payload = Bytes("yi er san si wu liu")});

    context.removeIOSocket(socket);

    return TestResult::Success;
}

TestResult basic_server_raw(std::string address_str)
{
    auto socket = bind_socket(address_str);

    socket->listen(5);  // Default backlog
    auto client = socket->accept();
    client->write_message("server");
    auto client_identity = client->read_message();
    RETURN_FAILURE_IF_FALSE(client_identity == "client");
    auto msg = client->read_message();
    RETURN_FAILURE_IF_FALSE(msg == "yi er san si wu liu");

    return TestResult::Success;
}

TestResult basic_client_raw(std::string address_str)
{
    auto socket = connect_socket(address_str);

    socket->write_message("client");
    auto server_identity = socket->read_message();
    RETURN_FAILURE_IF_FALSE(server_identity == "server");
    socket->write_message("yi er san si wu liu");

    return TestResult::Success;
}

TestResult server_receives_big_message(std::string address)
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Binder, "server");
    syncBindSocket(socket, address);
    auto result = syncRecvMessage(socket);

    RETURN_FAILURE_IF_FALSE(result.has_value());
    RETURN_FAILURE_IF_FALSE(result->payload.len() == 500'000'000);

    context.removeIOSocket(socket);

    return TestResult::Success;
}

TestResult client_sends_big_message(std::string address_str)
{
    auto socket = connect_socket(address_str);

    socket->write_message("client");
    auto remote_identity = socket->read_message();
    RETURN_FAILURE_IF_FALSE(remote_identity == "server");
    std::string msg(500'000'000, '.');
    socket->write_message(msg);

    return TestResult::Success;
}

TestResult reconnect_server_main(std::string address)
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Binder, "server");
    syncBindSocket(socket, address);
    auto result = syncRecvMessage(socket);

    RETURN_FAILURE_IF_FALSE(result.has_value());
    RETURN_FAILURE_IF_FALSE(result->payload.as_string() == "sync");

    auto error = syncSendMessage(socket, {.address = Bytes("client"), .payload = Bytes("acknowledge")});
    RETURN_FAILURE_IF_FALSE(!error);

    context.removeIOSocket(socket);

    return TestResult::Success;
}

TestResult reconnect_client_main(std::string address)
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Connector, "client");
    syncConnectSocket(socket, address);

    // create the recv future in advance, this remains active between reconnects
    auto future = futureRecvMessage(socket);

    // send "sync" and wait for "acknowledge" in a loop
    // the mitm will send a RST after the first "sync"
    // the "sync" message will be lost, but YMQ should automatically reconnect
    // therefore the next "sync" message should succeed
    for (size_t i = 0; i < 10; i++) {
        auto error = syncSendMessage(socket, {.address = Bytes("server"), .payload = Bytes("sync")});
        RETURN_FAILURE_IF_FALSE(!error);

        auto result = future.wait_for(1s);
        if (result == std::future_status::ready) {
            auto msg = future.get();
            RETURN_FAILURE_IF_FALSE(msg.has_value());
            RETURN_FAILURE_IF_FALSE(msg->payload.as_string() == "acknowledge");
            context.removeIOSocket(socket);

            return TestResult::Success;
        } else if (result == std::future_status::timeout) {
            // timeout, try again
            continue;
        } else {
            std::cerr << "future status error\n";
            return TestResult::Failure;
        }
    }

    std::cerr << "failed to reconnect after 10 attempts\n";
    return TestResult::Failure;
}

TestResult client_simulated_slow_network(std::string address)
{
    auto socket = connect_socket(address);

    socket->write_message("client");
    auto remote_identity = socket->read_message();
    RETURN_FAILURE_IF_FALSE(remote_identity == "server");

    std::string message = "yi er san si wu liu";
    uint64_t header     = message.length();

    socket->write_all((char*)&header, 4);
    std::this_thread::sleep_for(2s);
    socket->write_all((char*)&header + 4, 4);
    std::this_thread::sleep_for(3s);
    socket->write_all(message.data(), header / 2);
    std::this_thread::sleep_for(2s);
    socket->write_all(message.data() + header / 2, header - header / 2);

    return TestResult::Success;
}

TestResult client_sends_incomplete_identity(std::string address)
{
    // open a socket, write an incomplete identity and exit
    {
        auto socket = connect_socket(address);

        auto server_identity = socket->read_message();
        RETURN_FAILURE_IF_FALSE(server_identity == "server");

        // write incomplete identity and exit
        std::string identity = "client";
        uint64_t header      = identity.length();
        socket->write_all((char*)&header, 8);
        socket->write_all(identity.data(), identity.length() - 2);
    }

    // connect again and try to send a message
    {
        auto socket = connect_socket(address);

        auto server_identity = socket->read_message();
        RETURN_FAILURE_IF_FALSE(server_identity == "server");
        socket->write_message("client");
        socket->write_message("yi er san si wu liu");
    }

    return TestResult::Success;
}

TestResult server_receives_huge_header(std::string address)
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Binder, "server");
    syncBindSocket(socket, address);
    auto result = syncRecvMessage(socket);

    RETURN_FAILURE_IF_FALSE(result.has_value());
    RETURN_FAILURE_IF_FALSE(result->payload.as_string() == "yi er san si wu liu");

    context.removeIOSocket(socket);

    return TestResult::Success;
}

TestResult client_sends_huge_header(std::string address)
{
#ifdef __linux__
    // ignore SIGPIPE so that write() returns EPIPE instead of crashing the program
    signal(SIGPIPE, SIG_IGN);

    int expected_error = EPIPE;
#endif
#ifdef _WIN32
    int expected_error = WSAECONNABORTED;
#endif  // _WIN32

    {
        auto socket = connect_socket(address);

        socket->write_message("client");
        auto server_identity = socket->read_message();
        RETURN_FAILURE_IF_FALSE(server_identity == "server");

        // write the huge header
        uint64_t header = std::numeric_limits<uint64_t>::max();
        socket->write_all((char*)&header, 8);

        size_t i = 0;
        for (; i < 10; i++) {
            std::this_thread::sleep_for(1s);

            try {
                socket->write_all("yi er san si wu liu");
            } catch (const std::system_error& e) {
                if (e.code().value() == expected_error) {
                    std::cout << "writing failed, as expected after sending huge header, continuing...\n";
                    break;  // this is expected
                }

                throw;  // rethrow other errors
            }

            if (i == 10) {
                std::cout << "expected EPIPE after sending huge header\n";
                return TestResult::Failure;
            }
        }

        {
            auto socket = connect_socket(address);

            socket->write_message("client");
            auto server_identity = socket->read_message();
            RETURN_FAILURE_IF_FALSE(server_identity == "server");
            socket->write_message("yi er san si wu liu");
        }

        return TestResult::Success;
    }
}

TestResult server_receives_empty_messages(std::string address)
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Binder, "server");
    syncBindSocket(socket, address);

    auto result = syncRecvMessage(socket);
    RETURN_FAILURE_IF_FALSE(result.has_value());
    RETURN_FAILURE_IF_FALSE(result->payload.as_string() == "");

    auto result2 = syncRecvMessage(socket);
    RETURN_FAILURE_IF_FALSE(result2.has_value());
    RETURN_FAILURE_IF_FALSE(result2->payload.as_string() == "");

    context.removeIOSocket(socket);

    return TestResult::Success;
}

TestResult client_sends_empty_messages(std::string address)
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Connector, "client");
    syncConnectSocket(socket, address);

    auto error = syncSendMessage(socket, Message {.address = Bytes(), .payload = Bytes()});
    RETURN_FAILURE_IF_FALSE(!error);

    auto error2 = syncSendMessage(socket, Message {.address = Bytes(), .payload = Bytes("")});
    RETURN_FAILURE_IF_FALSE(!error2);

    context.removeIOSocket(socket);

    return TestResult::Success;
}

TestResult pubsub_subscriber(std::string address, std::string topic, int differentiator, void* sem)
{
    IOContext context(1);

    auto socket =
        syncCreateSocket(context, IOSocketType::Unicast, std::format("{}_subscriber_{}", topic, differentiator));

    std::this_thread::sleep_for(500ms);

    syncConnectSocket(socket, address);

    std::this_thread::sleep_for(500ms);

#ifdef __linux__
    if (sem_post((sem_t*)sem) < 0)
        throw std::system_error(errno, std::generic_category(), "failed to signal semaphore");
    sem_close((sem_t*)sem);
#endif  // __linux__
#ifdef _WIN32
    if (!ReleaseSemaphore(sem, 1, nullptr))
        throw std::system_error(GetLastError(), std::generic_category(), "failed to signal semaphore");
#endif  // _WIN32
    auto msg = syncRecvMessage(socket);
    RETURN_FAILURE_IF_FALSE(msg.has_value());
    RETURN_FAILURE_IF_FALSE(msg->payload.as_string() == "hello");

    context.removeIOSocket(socket);
    return TestResult::Success;
}

// topic: the identifier of the topic, must match what's passed to the subscribers
// sem: a semaphore to synchronize the publisher and subscriber processes
// n: the number of subscribers
TestResult pubsub_publisher(std::string address, std::string topic, void* sem, int n)
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Multicast, "publisher");
    syncBindSocket(socket, address);

// wait for the subscribers to be ready
#ifdef __linux__
    for (int i = 0; i < n; i++)
        if (sem_wait((sem_t*)sem) < 0)
            throw std::system_error(errno, std::generic_category(), "failed to wait on semaphore");
    sem_close((sem_t*)sem);
#endif  // __linux__
#ifdef _WIN32
    for (int i = 0; i < n; i++)
        if (WaitForSingleObject(sem, 3000) != WAIT_OBJECT_0)
            throw std::system_error(GetLastError(), std::generic_category(), "failed to wait on semaphore");
#endif  // _WIN32
        // the topic doesn't match, so no one should receive this
    auto error = syncSendMessage(
        socket, Message {.address = Bytes(std::format("x{}", topic)), .payload = Bytes("no one should get this")});
    RETURN_FAILURE_IF_FALSE(!error);

    // no one should receive this either
    error = syncSendMessage(
        socket,
        Message {.address = Bytes(std::format("{}x", topic)), .payload = Bytes("no one should get this either")});
    RETURN_FAILURE_IF_FALSE(!error);

    error = syncSendMessage(socket, Message {.address = Bytes(topic), .payload = Bytes("hello")});
    RETURN_FAILURE_IF_FALSE(!error);

    context.removeIOSocket(socket);
    return TestResult::Success;
}

TestResult client_close_established_connection_client(std::string address)
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Connector, "client");
    syncConnectSocket(socket, address);

    auto error = syncSendMessage(socket, Message {.address = Bytes("server"), .payload = Bytes("0")});
    RETURN_FAILURE_IF_FALSE(!error);
    auto result = syncRecvMessage(socket);
    RETURN_FAILURE_IF_FALSE(result.has_value());
    RETURN_FAILURE_IF_FALSE(result->payload.as_string() == "1");

    socket->closeConnection("server");
    context.requestIOSocketStop(socket);

    context.removeIOSocket(socket);
    return TestResult::Success;
}

TestResult client_close_established_connection_server(std::string address)
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Binder, "server");
    syncBindSocket(socket, address);

    auto error = syncSendMessage(socket, Message {.address = Bytes("client"), .payload = Bytes("1")});
    RETURN_FAILURE_IF_FALSE(!error);
    auto result = syncRecvMessage(socket);
    RETURN_FAILURE_IF_FALSE(result.has_value());
    RETURN_FAILURE_IF_FALSE(result->payload.as_string() == "0");

    result = syncRecvMessage(socket);
    RETURN_FAILURE_IF_FALSE(!result.has_value(), "expected recv message to fail");
    RETURN_FAILURE_IF_FALSE(
        result.error()._errorCode == scaler::ymq::Error::ErrorCode::ConnectorSocketClosedByRemoteEnd)

    context.removeIOSocket(socket);
    return TestResult::Success;
}

TestResult close_nonexistent_connection()
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Connector, "client");

    // note: we're not connected to anything; this connection does not exist
    // this should be a no-op..
    socket->closeConnection("server");

    context.removeIOSocket(socket);
    return TestResult::Success;
}

TestResult test_request_stop()
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Connector, "client");

    auto future = futureRecvMessage(socket);
    context.requestIOSocketStop(socket);

    auto result = future.wait_for(100ms);
    RETURN_FAILURE_IF_FALSE(result == std::future_status::ready, "future should have completed");

    // the future created beore requestion stop should have been cancelled with an error
    auto result2 = future.get();
    RETURN_FAILURE_IF_FALSE(!result2.has_value());
    RETURN_FAILURE_IF_FALSE(result2.error()._errorCode == scaler::ymq::Error::ErrorCode::IOSocketStopRequested);

    // and the same for any attempts to use the socket after it's been closed
    auto result3 = syncRecvMessage(socket);
    RETURN_FAILURE_IF_FALSE(!result3.has_value());
    RETURN_FAILURE_IF_FALSE(result3.error()._errorCode == scaler::ymq::Error::ErrorCode::IOSocketStopRequested);

    context.removeIOSocket(socket);
    return TestResult::Success;
}

TestResult client_socket_stop_before_close_connection(std::string address)
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Connector, "client");
    syncConnectSocket(socket, address);

    auto error = syncSendMessage(socket, Message {.address = Bytes("server"), .payload = Bytes("0")});
    RETURN_FAILURE_IF_FALSE(!error);
    auto result = syncRecvMessage(socket);
    RETURN_FAILURE_IF_FALSE(result.has_value());
    RETURN_FAILURE_IF_FALSE(result->payload.as_string() == "1");

    context.requestIOSocketStop(socket);
    socket->closeConnection("server");

    context.removeIOSocket(socket);
    return TestResult::Success;
}

TestResult server_socket_stop_before_close_connection(std::string address)
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Connector, "server");
    syncBindSocket(socket, address);

    auto error = syncSendMessage(socket, Message {.address = Bytes("client"), .payload = Bytes("1")});
    RETURN_FAILURE_IF_FALSE(!error);
    auto result = syncRecvMessage(socket);
    RETURN_FAILURE_IF_FALSE(result.has_value());
    RETURN_FAILURE_IF_FALSE(result->payload.as_string() == "0");

    result = syncRecvMessage(socket);
    RETURN_FAILURE_IF_FALSE(!result.has_value(), "expected recv message to fail");
    RETURN_FAILURE_IF_FALSE(
        result.error()._errorCode == scaler::ymq::Error::ErrorCode::ConnectorSocketClosedByRemoteEnd)

    context.removeIOSocket(socket);
    return TestResult::Success;
}

// -------------
//   test cases
// -------------
// this is a 'basic' test which sends a single message from a client to a server
// in this variant, both the client and server are implemented using YMQ
//
// this case includes a _delay_
// this is a thread sleep that happens after the client sends the message, to delay the close() of the socket
// at the moment, if this delay is missing, YMQ will not shut down correctly
TEST_P(CcYmqTestSuiteParametrized, TestBasicYMQClientYMQServer)
{
    const auto address = GetAddress(2889);

    // this is the test harness, it accepts a timeout, a list of functions to run,
    // and an optional third argument used to coordinate the execution of python (for mitm)
    auto result = test(10, {[=] { return basic_client_ymq(address); }, [=] { return basic_server_ymq(address); }});

    // test() aggregates the results across all of the provided functions
    EXPECT_EQ(result, TestResult::Success);
}

// same as above, except YMQs protocol is directly implemented on top of a TCP socket
TEST_P(CcYmqTestSuiteParametrized, TestBasicRawClientYMQServer)
{
    const auto address = GetAddress(2891);

    // this is the test harness, it accepts a timeout, a list of functions to run,
    // and an optional third argument used to coordinate the execution of python (for mitm)
    auto result = test(10, {[=] { return basic_client_raw(address); }, [=] { return basic_server_ymq(address); }});

    // test() aggregates the results across all of the provided functions
    EXPECT_EQ(result, TestResult::Success);
}

TEST_P(CcYmqTestSuiteParametrized, TestBasicRawClientRawServer)
{
    const auto address = GetAddress(2892);

    // this is the test harness, it accepts a timeout, a list of functions to run,
    // and an optional third argument used to coordinate the execution of python (for mitm)
    auto result = test(10, {[=] { return basic_client_raw(address); }, [=] { return basic_server_raw(address); }});

    // test() aggregates the results across all of the provided functions
    EXPECT_EQ(result, TestResult::Success);
}

// this is the same as above, except that it has no delay before calling close() on the socket
TEST_P(CcYmqTestSuiteParametrized, TestBasicRawClientRawServerNoDelay)
{
    const auto address = GetAddress(2893);

    auto result = test(10, {[=] { return basic_client_raw(address); }, [=] { return basic_server_ymq(address); }});

    EXPECT_EQ(result, TestResult::Success);
}

TEST_P(CcYmqTestSuiteParametrized, TestBasicDelayYMQClientRawServer)
{
    const auto address = GetAddress(2894);

    // this is the test harness, it accepts a timeout, a list of functions to run,
    // and an optional third argument used to coordinate the execution of python (for mitm)
    auto result = test(10, {[=] { return basic_client_ymq(address); }, [=] { return basic_server_raw(address); }});

    // test() aggregates the results across all of the provided functions
    EXPECT_EQ(result, TestResult::Success);
}

// in this test case, the client sends a large message to the server
// YMQ should be able to handle this without issue
TEST_P(CcYmqTestSuiteParametrized, TestClientSendBigMessageToServer)
{
    const auto address = GetAddress(2895);

    auto result = test(
        10, {[=] { return client_sends_big_message(address); }, [=] { return server_receives_big_message(address); }});

    EXPECT_EQ(result, TestResult::Success);
}

// this is the no-op/passthrough man in the middle test
// for this test case we use YMQ on both the client side and the server side
// the client connects to the mitm, and the mitm connects to the server
// when the mitm receives packets from the client, it forwards it to the server without changing it
// and similarly when it receives packets from the server, it forwards them to the client
//
// the mitm is implemented in Python. we pass the name of the test case, which corresponds to the Python filename,
// and a list of arguments, which are: mitm ip, mitm port, remote ip, remote port
// this defines the address of the mitm, and the addresses that can connect to it
// for more, see the python mitm files
TEST(CcYmqTestSuite, TestMitmPassthrough)
{
#ifdef __linux__
    auto mitm_ip   = "192.0.2.4";
    auto remote_ip = "192.0.2.3";
#endif  // __linux__
#ifdef _WIN32
    auto mitm_ip   = "127.0.0.1";
    auto remote_ip = "127.0.0.1";
#endif  // _WIN32
    auto mitm_port   = random_port();
    auto remote_port = 23571;

    // the Python program must be the first and only the first function passed to test()
    // we must also pass `true` as the third argument to ensure that Python is fully started
    // before beginning the test
    auto result = test(
        20,
        {[=] { return run_mitm("passthrough", mitm_ip, mitm_port, remote_ip, remote_port); },
         [=] { return basic_client_ymq(std::format("tcp://{}:{}", mitm_ip, mitm_port)); },
         [=] { return basic_server_ymq(std::format("tcp://{}:{}", remote_ip, remote_port)); }},
        true);

    EXPECT_EQ(result, TestResult::Success);
}

// this is the same as the above, but both the client and server use raw sockets
TEST(CcYmqTestSuite, TestMitmPassthroughRaw)
{
#ifdef __linux__
    auto mitm_ip   = "192.0.2.4";
    auto remote_ip = "192.0.2.3";
#endif  // __linux__
#ifdef _WIN32
    auto mitm_ip   = "127.0.0.1";
    auto remote_ip = "127.0.0.1";
#endif  // _WIN32
    auto mitm_port   = random_port();
    auto remote_port = 23574;

    // the Python program must be the first and only the first function passed to test()
    // we must also pass `true` as the third argument to ensure that Python is fully started
    // before beginning the test
    auto result = test(
        20,
        {[=] { return run_mitm("passthrough", mitm_ip, mitm_port, remote_ip, remote_port); },
         [=] { return basic_client_ymq(std::format("tcp://{}:{}", mitm_ip, mitm_port)); },
         [=] { return basic_server_ymq(std::format("tcp://{}:{}", remote_ip, remote_port)); }},
        true);
    EXPECT_EQ(result, TestResult::Success);
}

// this test uses the mitm to test the reconnect logic of YMQ by sending RST packets
TEST(CcYmqTestSuite, TestMitmReconnect)
{
#ifdef __linux__
    auto mitm_ip   = "192.0.2.4";
    auto remote_ip = "192.0.2.3";
#endif  // __linux__
#ifdef _WIN32
    auto mitm_ip   = "127.0.0.1";
    auto remote_ip = "127.0.0.1";
#endif  // _WIN32
    auto mitm_port   = random_port();
    auto remote_port = 23572;

    auto result = test(
        30,
        {[=] { return run_mitm("send_rst_to_client", mitm_ip, mitm_port, remote_ip, remote_port); },
         [=] { return reconnect_client_main(std::format("tcp://{}:{}", mitm_ip, mitm_port)); },
         [=] { return reconnect_server_main(std::format("tcp://{}:{}", remote_ip, remote_port)); }},
        true);

    EXPECT_EQ(result, TestResult::Success);
}

// in this test, the mitm drops a random % of packets arriving from the client and server
TEST(CcYmqTestSuite, TestMitmRandomlyDropPackets)
{
#ifdef __linux__
    auto mitm_ip   = "192.0.2.4";
    auto remote_ip = "192.0.2.3";
#endif  // __linux__
#ifdef _WIN32
    auto mitm_ip   = "127.0.0.1";
    auto remote_ip = "127.0.0.1";
#endif  // _WIN32
    auto mitm_port   = random_port();
    auto remote_port = 23573;

    auto result = test(
        60,
        {[=] { return run_mitm("randomly_drop_packets", mitm_ip, mitm_port, remote_ip, remote_port, {"0.3"}); },
         [=] { return basic_client_ymq(std::format("tcp://{}:{}", mitm_ip, mitm_port)); },
         [=] { return basic_server_ymq(std::format("tcp://{}:{}", remote_ip, remote_port)); }},
        true);

    EXPECT_EQ(result, TestResult::Success);
}

// in this test the client is sending a message to the server
// but we simulate a slow network connection by sending the message in segmented chunks
TEST_P(CcYmqTestSuiteParametrized, TestSlowNetwork)
{
    const auto address = GetAddress(2905);

    auto result =
        test(20, {[=] { return client_simulated_slow_network(address); }, [=] { return basic_server_ymq(address); }});

    EXPECT_EQ(result, TestResult::Success);
}

// in this test, a client connects to the YMQ server but only partially sends its identity and then disconnects
// then a new client connection is established, and this one sends a complete identity and message
// YMQ should be able to recover from a poorly-behaved client like this
TEST_P(CcYmqTestSuiteParametrized, TestClientSendIncompleteIdentity)
{
    const auto address = GetAddress(2896);

    auto result = test(
        20, {[=] { return client_sends_incomplete_identity(address); }, [=] { return basic_server_ymq(address); }});

    EXPECT_EQ(result, TestResult::Success);
}

// in this test, the client sends an unrealistically-large header
// it is important that YMQ checks the header size before allocating memory
// both for resilence against attacks and to guard against errors
TEST_P(CcYmqTestSuiteParametrized, TestClientSendHugeHeader)
{
    const auto address = GetAddress(2897);

    auto result = test(
        20, {[=] { return client_sends_huge_header(address); }, [=] { return server_receives_huge_header(address); }});

    EXPECT_EQ(result, TestResult::Success);
}

// in this test, the client sends empty messages to the server
// there are in effect two kinds of empty messages: Bytes() and Bytes("")
// in the former case, the bytes contains a nullptr
// in the latter case, the bytes contains a zero-length allocation
// it's important that the behaviour of YMQ is known for both of these cases
TEST_P(CcYmqTestSuiteParametrized, TestClientSendEmptyMessage)
{
    const auto address = GetAddress(2898);

    auto result = test(
        20,
        {[=] { return client_sends_empty_messages(address); },
         [=] { return server_receives_empty_messages(address); }});

    EXPECT_EQ(result, TestResult::Success);
}

// this case tests the publish-subscribe pattern of YMQ
// we create one publisher and two subscribers with a common topic
// the publisher will send two messages to the wrong topic
// none of the subscribers should receive these
// and then the publisher will send a message to the correct topic
// both subscribers should receive this message
TEST_P(CcYmqTestSuiteParametrized, TestPubSub)
{
    const auto address = GetAddress(2900);
    auto topic         = "mytopic";

    // allocate a semaphore to synchronize the publisher and subscriber processes
#ifdef __linux__
    sem_t* sem =
        static_cast<sem_t*>(mmap(nullptr, sizeof(sem_t), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0));
    if (sem == MAP_FAILED)
        throw std::system_error(errno, std::generic_category(), "failed to map shared memory for semaphore");
    if (sem_init(sem, 1, 0) < 0)
        throw std::system_error(errno, std::generic_category(), "failed to initialize semaphore");
#endif  // __linux__
#ifdef _WIN32

    HANDLE sem = CreateSemaphore(

        nullptr,   // default security attributes
        0,         // initial count
        2,         // maximum count
        nullptr);  // unnamed semaphore
    if (sem == nullptr)

        throw std::system_error(GetLastError(), std::generic_category(), "failed to create semaphore");

#endif  // _WIN32
    auto result = test(
        20,
        {[=] { return pubsub_publisher(address, topic, sem, 2); },
         [=] { return pubsub_subscriber(address, topic, 0, sem); },
         [=] { return pubsub_subscriber(address, topic, 1, sem); }});

#ifdef __linux__
    sem_destroy(sem);
    munmap(sem, sizeof(sem_t));
#endif  // __linux__
#ifdef _WIN32
    CloseHandle(sem);
#endif  // _WIN32
    EXPECT_EQ(result, TestResult::Success);
}

// this sets the publisher with an empty topic and the subscribers with two other topics
// both subscribers should get all messages
TEST_P(CcYmqTestSuiteParametrized, TestPubSubEmptyTopic)
{
    const auto address = GetAddress(2906);

    // allocate a semaphore to synchronize the publisher and subscriber processes
#ifdef __linux__
    sem_t* sem =
        static_cast<sem_t*>(mmap(nullptr, sizeof(sem_t), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0));
    if (sem == MAP_FAILED)
        throw std::system_error(errno, std::generic_category(), "failed to map shared memory for semaphore");
    if (sem_init(sem, 1, 0) < 0)
        throw std::system_error(errno, std::generic_category(), "failed to initialize semaphore");

#endif  // __linux__
#ifdef _WIN32

    HANDLE sem = CreateSemaphore(

        nullptr,   // default security attributes
        0,         // initial count
        2,         // maximum count
        nullptr);  // unnamed semaphore
    if (sem == nullptr)

        throw std::system_error(GetLastError(), std::generic_category(), "failed to create semaphore");

#endif  // _WIN32
    auto result = test(
        20,
        {[=] { return pubsub_publisher(address, "", sem, 2); },
         [=] { return pubsub_subscriber(address, "abc", 0, sem); },
         [=] { return pubsub_subscriber(address, "def", 1, sem); }});

#ifdef __linux__
    sem_destroy(sem);
    munmap(sem, sizeof(sem_t));
#endif  // __linux__
#ifdef _WIN32
    CloseHandle(sem);
#endif  // _WIN32
    EXPECT_EQ(result, TestResult::Success);
}

// in this test case, the client establishes a connection with the server and then explicitly closes it
TEST_P(CcYmqTestSuiteParametrized, DISABLED_TestClientCloseEstablishedConnection)

{
    const auto address = GetAddress(2902);

    auto result = test(
        20,
        {[=] { return client_close_established_connection_client(address); },
         [=] { return client_close_established_connection_server(address); }});

    EXPECT_EQ(result, TestResult::Success);
}

// this test case is similar to the one above, except that it requests the socket stop before closing the connection
TEST_P(CcYmqTestSuiteParametrized, TestClientSocketStopBeforeCloseConnection)

{
    const auto address = GetAddress(2904);

    auto result = test(
        20,
        {[=] { return client_socket_stop_before_close_connection(address); },
         [=] { return server_socket_stop_before_close_connection(address); }});

    EXPECT_EQ(result, TestResult::Success);
}

// in this test case, the we try to close a connection that does not exist
TEST(CcYmqTestSuite, TestClientCloseNonexistentConnection)

{
    auto result = close_nonexistent_connection();
    EXPECT_EQ(result, TestResult::Success);
}

// this test case verifies that requesting a socket stop causes pending and subsequent operations to be cancelled
TEST(CcYmqTestSuite, TestRequestSocketStop)
{
    auto result = test_request_stop();
    EXPECT_EQ(result, TestResult::Success);
}

std::vector<std::string> GetTransports()
{
    std::vector<std::string> transports;
    transports.push_back("tcp");
#ifdef __linux__
    transports.push_back("ipc");
#endif
    return transports;
}

// parametrize the test with tcp and ipc addresses
INSTANTIATE_TEST_SUITE_P(
    YMQTransport,
    CcYmqTestSuiteParametrized,
    ::testing::ValuesIn(GetTransports()),
    [](const testing::TestParamInfo<CcYmqTestSuiteParametrized::ParamType>& info) {
        // use tcp/ipc as suffix for test names
        return info.param;
    });

// main
int main(int argc, char** argv)
{
    ensure_python_initialized();

#ifdef _WIN32
    // initialize winsock
    WSADATA wsaData = {};
    int iResult     = WSAStartup(MAKEWORD(2, 2), &wsaData);
    if (iResult != 0) {
        std::cerr << "WSAStartup failed: " << iResult << "\n";
        return 1;
    }
#endif  // _WIN32

    testing::InitGoogleTest(&argc, argv);
    auto result = RUN_ALL_TESTS();

#ifdef _WIN32
    WSACleanup();
#endif  // _WIN32

    maybe_finalize_python();
    return result;
}

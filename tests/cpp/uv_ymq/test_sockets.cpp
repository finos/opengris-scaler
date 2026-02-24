#include <gtest/gtest.h>

#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <format>
#include <future>
#include <iostream>
#include <limits>
#include <string>
#include <system_error>
#include <thread>

#include "scaler/error/error.h"
#include "scaler/uv_ymq/address.h"
#include "scaler/uv_ymq/future/binder_socket.h"
#include "scaler/uv_ymq/future/connector_socket.h"
#include "scaler/uv_ymq/io_context.h"
#include "scaler/uv_ymq/sync/binder_socket.h"
#include "scaler/uv_ymq/sync/connector_socket.h"
#include "scaler/ymq/bytes.h"
#include "tests/cpp/ymq/common/testing.h"
#include "tests/cpp/ymq/common/utils.h"
#include "tests/cpp/ymq/net/socket_utils.h"

using scaler::uv_ymq::Address;
using scaler::uv_ymq::Identity;
using scaler::uv_ymq::IOContext;
using scaler::ymq::Bytes;
using scaler::ymq::Error;
using scaler::ymq::Message;

// a test suite for different socket types that's parameterized by transport protocol (e.g. "tcp", "ipc")
class UVYMQSocketTest: public ::testing::TestWithParam<std::string> {
protected:
    std::string GetAddress(int port)
    {
        const std::string& transport = GetParam();
        if (transport == "tcp") {
            return std::format("tcp://127.0.0.1:{}", port);
        }
        if (transport == "ipc") {
            // using a unique path for each test based on port
            const char* runner_temp = std::getenv("RUNNER_TEMP");
            if (runner_temp) {
                return std::format("ipc://{}/ymq-test-{}.ipc", runner_temp, port);
            }
            return std::format("ipc:///tmp/ymq-test-{}.ipc", port);
        }
        // Gtest should not select this for unsupported platforms, but as a fallback,
        // return something that will cause tests to fail clearly.
        return "invalid-transport";
    }
};

// --------------------
//  clients and servers
// --------------------

TestResult basicServerYmq(std::string address)
{
    IOContext context {};

    scaler::uv_ymq::sync::BinderSocket socket {context, "server"};
    auto bindResult = socket.bindTo(address);
    RETURN_FAILURE_IF_FALSE(bindResult.has_value());

    auto result = socket.recvMessage();

    RETURN_FAILURE_IF_FALSE(result.has_value());
    RETURN_FAILURE_IF_FALSE(result->payload.as_string() == "yi er san si wu liu");

    return TestResult::Success;
}

TestResult basicClientYmq(std::string address)
{
    IOContext context {};

    auto socketResult = scaler::uv_ymq::sync::ConnectorSocket::init(context, "client", address);
    RETURN_FAILURE_IF_FALSE(socketResult.has_value());

    auto socket     = std::move(socketResult.value());
    auto sendResult = socket.sendMessage(Bytes {"yi er san si wu liu"});
    RETURN_FAILURE_IF_FALSE(sendResult.has_value());

    // Reading a message should fail as the server will immediately shutdown after receiving our message
    auto readResult = socket.recvMessage();
    RETURN_FAILURE_IF_FALSE(!readResult.has_value());
    RETURN_FAILURE_IF_FALSE(readResult.error()._errorCode == Error::ErrorCode::ConnectorSocketClosedByRemoteEnd);

    return TestResult::Success;
}

TestResult basicServerRaw(std::string address_str)
{
    auto socket = bindSocket(address_str);

    socket->listen(5);  // Default backlog
    auto client = socket->accept();
    client->writeMessage("server");
    auto client_identity = client->readMessage();
    RETURN_FAILURE_IF_FALSE(client_identity == "client");
    auto msg = client->readMessage();
    RETURN_FAILURE_IF_FALSE(msg == "yi er san si wu liu");

    return TestResult::Success;
}

TestResult basicClientRaw(std::string address_str)
{
    auto socket = connectSocket(address_str);

    socket->writeMessage("client");
    auto server_identity = socket->readMessage();
    RETURN_FAILURE_IF_FALSE(server_identity == "server");
    socket->writeMessage("yi er san si wu liu");

    return TestResult::Success;
}

TestResult serverReceivesBigMessage(std::string address)
{
    IOContext context {};

    scaler::uv_ymq::sync::BinderSocket socket {context, "server"};
    auto bindResult = socket.bindTo(address);
    RETURN_FAILURE_IF_FALSE(bindResult.has_value());

    auto result = socket.recvMessage();

    RETURN_FAILURE_IF_FALSE(result.has_value());
    RETURN_FAILURE_IF_FALSE(result->payload.len() == 500'000'000);

    return TestResult::Success;
}

TestResult clientSendsBigMessage(std::string address_str)
{
    auto socket = connectSocket(address_str);

    socket->writeMessage("client");
    auto remote_identity = socket->readMessage();
    RETURN_FAILURE_IF_FALSE(remote_identity == "server");
    std::string msg(500'000'000, '.');
    socket->writeMessage(msg);

    return TestResult::Success;
}

TestResult clientSimulatedSlowNetwork(std::string address)
{
    auto socket = connectSocket(address);

    socket->writeMessage("client");
    auto remote_identity = socket->readMessage();
    RETURN_FAILURE_IF_FALSE(remote_identity == "server");

    std::string message = "yi er san si wu liu";
    uint64_t header     = message.length();

    socket->writeAll((char*)&header, 4);
    std::this_thread::sleep_for(std::chrono::seconds {2});
    socket->writeAll((char*)&header + 4, 4);
    std::this_thread::sleep_for(std::chrono::seconds {3});
    socket->writeAll(message.data(), header / 2);
    std::this_thread::sleep_for(std::chrono::seconds {2});
    socket->writeAll(message.data() + header / 2, header - header / 2);

    return TestResult::Success;
}

TestResult clientSendsIncompleteIdentity(std::string address)
{
    // open a socket, write an incomplete identity and exit
    {
        auto socket = connectSocket(address);

        auto server_identity = socket->readMessage();
        RETURN_FAILURE_IF_FALSE(server_identity == "server");

        // write incomplete identity and exit
        std::string identity = "client";
        uint64_t header      = identity.length();
        socket->writeAll((char*)&header, 8);
        socket->writeAll(identity.data(), identity.length() - 2);
    }

    // connect again and try to send a message
    {
        auto socket = connectSocket(address);

        auto server_identity = socket->readMessage();
        RETURN_FAILURE_IF_FALSE(server_identity == "server");
        socket->writeMessage("client");
        socket->writeMessage("yi er san si wu liu");
    }

    return TestResult::Success;
}

TestResult serverReceivesHugeHeader(std::string address)
{
    IOContext context {};

    scaler::uv_ymq::sync::BinderSocket socket {context, "server"};
    auto bindResult = socket.bindTo(address);
    RETURN_FAILURE_IF_FALSE(bindResult.has_value());

    auto result = socket.recvMessage();

    RETURN_FAILURE_IF_FALSE(result.has_value());
    RETURN_FAILURE_IF_FALSE(result->payload.as_string() == "yi er san si wu liu");

    return TestResult::Success;
}

TestResult clientSendsHugeHeader(std::string address)
{
    {
        auto socket = connectSocket(address);

        socket->writeMessage("client");
        auto server_identity = socket->readMessage();
        RETURN_FAILURE_IF_FALSE(server_identity == "server");

        // write the huge header
        uint64_t header = std::numeric_limits<uint64_t>::max();
        socket->writeAll((char*)&header, sizeof(header));

        size_t i = 0;
        for (; i < 10; i++) {
            std::this_thread::sleep_for(std::chrono::seconds {1});

            try {
                socket->writeAll("yi er san si wu liu");
            } catch (const std::system_error& e) {
                // Expected to fail after sending huge header
                std::cout << "writing failed, as expected after sending huge header, continuing...\n";
                break;
            }

            if (i == 10) {
                std::cout << "expected write error after sending huge header\n";
                return TestResult::Failure;
            }
        }

        {
            auto socket = connectSocket(address);

            socket->writeMessage("client");
            auto server_identity = socket->readMessage();
            RETURN_FAILURE_IF_FALSE(server_identity == "server");
            socket->writeMessage("yi er san si wu liu");
        }

        return TestResult::Success;
    }
}

TestResult serverReceivesEmptyMessages(std::string address)
{
    IOContext context {};

    scaler::uv_ymq::sync::BinderSocket socket {context, "server"};
    auto bindResult = socket.bindTo(address);
    RETURN_FAILURE_IF_FALSE(bindResult.has_value());

    auto result = socket.recvMessage();
    RETURN_FAILURE_IF_FALSE(result.has_value());
    RETURN_FAILURE_IF_FALSE(result->payload.as_string() == "");

    auto error = socket.sendMessage("client", Bytes {""});
    RETURN_FAILURE_IF_FALSE(error.has_value());

    return TestResult::Success;
}

TestResult clientSendsEmptyMessages(std::string address)
{
    IOContext context {};

    auto socketResult = scaler::uv_ymq::sync::ConnectorSocket::init(context, "client", address);
    RETURN_FAILURE_IF_FALSE(socketResult.has_value());

    auto socket = std::move(socketResult.value());

    auto error = socket.sendMessage(Bytes {""});
    RETURN_FAILURE_IF_FALSE(error.has_value());

    auto result = socket.recvMessage();
    RETURN_FAILURE_IF_FALSE(result.has_value());
    RETURN_FAILURE_IF_FALSE(result->payload.as_string() == "");

    return TestResult::Success;
}

// TODO: Multicast/Unicast sockets are not yet implemented in uv_ymq

/*
TestResult pubsubSubscriber(std::string address, std::string topic, int differentiator, void* sem)
{
    IOContext context{};

    // auto socket = scaler::uv_ymq::sync::UnicastSocket{
    //     context, std::format("{}_subscriber_{}", topic, differentiator)};

    std::this_thread::sleep_for(std::chrono::milliseconds{500});

    // Connect to address

    std::this_thread::sleep_for(std::chrono::milliseconds{500});

    // Receive message
    // auto msg = socket.recvMessage();
    // RETURN_FAILURE_IF_FALSE(msg.has_value());
    // RETURN_FAILURE_IF_FALSE(msg->payload.as_string() == "hello");

    return TestResult::Success;
}

TestResult pubsubPublisher(std::string address, std::string topic, void* sem, int n)
{
    IOContext context{};

    // Implement Multicast socket type
    // auto socket = scaler::uv_ymq::sync::MulticastSocket{context, "publisher"};
    // auto bindResult = socket.bindTo(address);
    // RETURN_FAILURE_IF_FALSE(bindResult.has_value());

    // Wait for subscribers to be ready using semaphore

    // Send messages to wrong topics
    // auto error = socket.sendMessage(std::format("x{}", topic), Bytes{"no one should get this"});
    // RETURN_FAILURE_IF_FALSE(error.has_value());

    // error = socket.sendMessage(std::format("{}x", topic), Bytes{"no one should get this either"});
    // RETURN_FAILURE_IF_FALSE(error.has_value());

    // Send message to correct topic
    // error = socket.sendMessage(topic, Bytes{"hello"});
    // RETURN_FAILURE_IF_FALSE(error.has_value());

    return TestResult::Success;
}
*/

TestResult clientCloseEstablishedConnectionClient(std::string address)
{
    IOContext context {};

    auto socketResult = scaler::uv_ymq::sync::ConnectorSocket::init(context, "client", address);
    RETURN_FAILURE_IF_FALSE(socketResult.has_value());

    auto socket = std::move(socketResult.value());

    auto error = socket.sendMessage(Bytes {"0"});
    RETURN_FAILURE_IF_FALSE(error.has_value());

    auto result = socket.recvMessage();
    RETURN_FAILURE_IF_FALSE(result.has_value());
    RETURN_FAILURE_IF_FALSE(result->payload.as_string() == "1");

    result = socket.recvMessage();
    RETURN_FAILURE_IF_FALSE(!result.has_value(), "expected recv message to fail");
    RETURN_FAILURE_IF_FALSE(result.error()._errorCode == Error::ErrorCode::ConnectorSocketClosedByRemoteEnd)

    return TestResult::Success;
}

TestResult clientCloseEstablishedConnectionServer(std::string address)
{
    IOContext context {};

    scaler::uv_ymq::sync::BinderSocket socket {context, "server"};
    auto bindResult = socket.bindTo(address);
    RETURN_FAILURE_IF_FALSE(bindResult.has_value());

    auto error = socket.sendMessage("client", Bytes {"1"});
    RETURN_FAILURE_IF_FALSE(error.has_value());

    auto result = socket.recvMessage();
    RETURN_FAILURE_IF_FALSE(result.has_value());
    RETURN_FAILURE_IF_FALSE(result->payload.as_string() == "0");

    socket.closeConnection("client");

    return TestResult::Success;
}

TestResult closeNonexistentConnection()
{
    IOContext context {};

    scaler::uv_ymq::sync::BinderSocket socket {context, "server"};

    // note: we're not connected to anything; this connection does not exist
    // this should be a no-op
    socket.closeConnection("client");

    return TestResult::Success;
}

TestResult testRequestStop()
{
    IOContext context {};

    std::future<std::expected<scaler::ymq::Message, Error>> future;

    {
        auto binder = scaler::uv_ymq::future::BinderSocket {context, "server"};

        future = binder.recvMessage();

        // Socket destructor will be called here, canceling pending operations
    }

    RETURN_FAILURE_IF_FALSE(
        future.wait_for(std::chrono::milliseconds {100}) == std::future_status::ready, "future should have completed");

    // The future created before stopping the socket should have been cancelled with an error
    auto result = future.get();
    RETURN_FAILURE_IF_FALSE(!result.has_value());
    RETURN_FAILURE_IF_FALSE(result.error()._errorCode == Error::ErrorCode::IOSocketStopRequested);

    return TestResult::Success;
}

TestResult clientSocketStopBeforeCloseConnection(std::string address)
{
    IOContext context {};

    auto socketResult = scaler::uv_ymq::sync::ConnectorSocket::init(context, "client", address);
    RETURN_FAILURE_IF_FALSE(socketResult.has_value());

    auto socket = std::move(socketResult.value());

    auto error = socket.sendMessage(Bytes {"0"});
    RETURN_FAILURE_IF_FALSE(error.has_value());

    auto result = socket.recvMessage();
    RETURN_FAILURE_IF_FALSE(result.has_value());
    RETURN_FAILURE_IF_FALSE(result->payload.as_string() == "1");

    result = socket.recvMessage();
    RETURN_FAILURE_IF_FALSE(!result.has_value(), "expected recv message to fail");
    RETURN_FAILURE_IF_FALSE(result.error()._errorCode == Error::ErrorCode::ConnectorSocketClosedByRemoteEnd)

    return TestResult::Success;
}

TestResult serverSocketStopBeforeCloseConnection(std::string address)
{
    IOContext context {};

    {
        scaler::uv_ymq::sync::BinderSocket socket {context, "server"};
        auto bindResult = socket.bindTo(address);
        RETURN_FAILURE_IF_FALSE(bindResult.has_value());

        auto error = socket.sendMessage("client", Bytes {"1"});
        RETURN_FAILURE_IF_FALSE(error.has_value());

        auto result = socket.recvMessage();
        RETURN_FAILURE_IF_FALSE(result.has_value());
        RETURN_FAILURE_IF_FALSE(result->payload.as_string() == "0");

        // The socket will be stopped when it's destroyed
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    return TestResult::Success;
}

// -------------
//   test cases
// -------------

// this is a 'basic' test which sends a single message from a client to a server
// in this variant, both the client and server are implemented using uv_ymq
TEST_P(UVYMQSocketTest, TestBasicYMQClientYMQServer)
{
    const auto address = GetAddress(2889);

    // this is the test harness, it accepts a timeout, a list of functions to run,
    // and an optional third argument used to coordinate the execution of python (for mitm)
    auto result = test(10, {[=] { return basicServerYmq(address); }, [=] { return basicClientYmq(address); }});

    // test() aggregates the results across all of the provided functions
    EXPECT_EQ(result, TestResult::Success);
}

// same as above, except uv_ymq's protocol is directly implemented on top of a TCP socket
TEST_P(UVYMQSocketTest, TestBasicRawClientYMQServer)
{
    const auto address = GetAddress(2891);

    // this is the test harness, it accepts a timeout, a list of functions to run,
    // and an optional third argument used to coordinate the execution of python (for mitm)
    auto result = test(10, {[=] { return basicServerYmq(address); }, [=] { return basicClientRaw(address); }});

    // test() aggregates the results across all of the provided functions
    EXPECT_EQ(result, TestResult::Success);
}

TEST_P(UVYMQSocketTest, TestBasicRawClientRawServer)
{
    const auto address = GetAddress(2892);

    // this is the test harness, it accepts a timeout, a list of functions to run,
    // and an optional third argument used to coordinate the execution of python (for mitm)
    auto result = test(10, {[=] { return basicServerRaw(address); }, [=] { return basicClientRaw(address); }});

    // test() aggregates the results across all of the provided functions
    EXPECT_EQ(result, TestResult::Success);
}

// this is the same as above, except that it has no delay before calling close() on the socket
TEST_P(UVYMQSocketTest, TestBasicRawClientRawServerNoDelay)
{
    const auto address = GetAddress(2893);

    auto result = test(10, {[=] { return basicServerYmq(address); }, [=] { return basicClientRaw(address); }});

    EXPECT_EQ(result, TestResult::Success);
}

TEST_P(UVYMQSocketTest, TestBasicDelayYMQClientRawServer)
{
    const auto address = GetAddress(2894);

    // this is the test harness, it accepts a timeout, a list of functions to run,
    // and an optional third argument used to coordinate the execution of python (for mitm)
    auto result = test(10, {[=] { return basicServerRaw(address); }, [=] { return basicClientYmq(address); }});

    // test() aggregates the results across all of the provided functions
    EXPECT_EQ(result, TestResult::Success);
}

// in this test case, the client sends a large message to the server
// uv_ymq should be able to handle this without issue
TEST_P(UVYMQSocketTest, TestClientSendBigMessageToServer)
{
    const auto address = GetAddress(2895);

    auto result =
        test(10, {[=] { return serverReceivesBigMessage(address); }, [=] { return clientSendsBigMessage(address); }});

    EXPECT_EQ(result, TestResult::Success);
}

// in this test the client is sending a message to the server
// but we simulate a slow network connection by sending the message in segmented chunks
TEST_P(UVYMQSocketTest, TestSlowNetwork)
{
    const auto address = GetAddress(2905);

    auto result =
        test(20, {[=] { return basicServerYmq(address); }, [=] { return clientSimulatedSlowNetwork(address); }});

    EXPECT_EQ(result, TestResult::Success);
}

// in this test, a client connects to the YMQ server but only partially sends its identity and then disconnects
// then a new client connection is established, and this one sends a complete identity and message
// YMQ should be able to recover from a poorly-behaved client like this
TEST_P(UVYMQSocketTest, TestClientSendIncompleteIdentity)
{
    const auto address = GetAddress(2896);

    auto result =
        test(20, {[=] { return basicServerYmq(address); }, [=] { return clientSendsIncompleteIdentity(address); }});

    EXPECT_EQ(result, TestResult::Success);
}

// in this test, the client sends an unrealistically-large header
// it is important that YMQ checks the header size before allocating memory
// both for resilience against attacks and to guard against errors
TEST_P(UVYMQSocketTest, TestClientSendHugeHeader)
{
    const auto address = GetAddress(2897);

    auto result =
        test(20, {[=] { return serverReceivesHugeHeader(address); }, [=] { return clientSendsHugeHeader(address); }});

    EXPECT_EQ(result, TestResult::Success);
}

// in this test, the client sends empty messages to the server
// there are in effect two kinds of empty messages: Bytes() and Bytes("")
// in the former case, the bytes contains a nullptr
// in the latter case, the bytes contains a zero-length allocation
// it's important that the behaviour of YMQ is known for both of these cases
TEST_P(UVYMQSocketTest, TestClientSendEmptyMessage)
{
    const auto address = GetAddress(2898);

    auto result = test(
        20, {[=] { return serverReceivesEmptyMessages(address); }, [=] { return clientSendsEmptyMessages(address); }});

    EXPECT_EQ(result, TestResult::Success);
}

// this case tests the publish-subscribe pattern of uv_ymq
// we create one publisher and two subscribers with a common topic
// the publisher will send two messages to the wrong topic
// none of the subscribers should receive these
// and then the publisher will send a message to the correct topic
// both subscribers should receive this message
//
// NOTE: Multicast/Unicast sockets are not yet implemented
/*
TEST_P(UVYMQSocketTest, TestPubSub)
{
    const auto address = GetAddress(2900);
    auto topic         = "mytopic";

    // TODO: Implement cross-platform semaphore allocation

    auto result = test(
        20,
        {[=] { return pubsubPublisher(address, topic, sem, 2); },
         [=] { return pubsubSubscriber(address, topic, 0, sem); },
         [=] { return pubsubSubscriber(address, topic, 1, sem); }});

    // TODO: Cleanup semaphore

    EXPECT_EQ(result, TestResult::Success);
}
*/

// this sets the publisher with an empty topic and the subscribers with two other topics
// both subscribers should get all messages
//
// NOTE: Multicast/Unicast sockets are not yet implemented
/*
TEST_P(UVYMQSocketTest, TestPubSubEmptyTopic)
{
    const auto address = GetAddress(2906);

    // TODO: Implement cross-platform semaphore allocation

    auto result = test(
        20,
        {[=] { return pubsubPublisher(address, "", sem, 2); },
         [=] { return pubsubSubscriber(address, "abc", 0, sem); },
         [=] { return pubsubSubscriber(address, "def", 1, sem); }});

    // TODO: Cleanup semaphore

    EXPECT_EQ(result, TestResult::Success);
}
*/

// in this test case, the client establishes a connection with the server and then explicitly closes it
TEST_P(UVYMQSocketTest, TestClientCloseEstablishedConnection)
{
    const auto address = GetAddress(2902);

    auto result = test(
        20,
        {[=] { return clientCloseEstablishedConnectionServer(address); },
         [=] { return clientCloseEstablishedConnectionClient(address); }});

    EXPECT_EQ(result, TestResult::Success);
}

// this test case is similar to the one above, except that it requests the socket stop before closing the connection
TEST_P(UVYMQSocketTest, TestClientSocketStopBeforeCloseConnection)
{
    const auto address = GetAddress(2904);

    auto result = test(
        20,
        {[=] { return serverSocketStopBeforeCloseConnection(address); },
         [=] { return clientSocketStopBeforeCloseConnection(address); }});

    EXPECT_EQ(result, TestResult::Success);
}

// in this test case, the we try to close a connection that does not exist
TEST(UVYMQSocketTest, TestClientCloseNonexistentConnection)
{
    auto result = closeNonexistentConnection();
    EXPECT_EQ(result, TestResult::Success);
}

// this test case verifies that requesting a socket stop causes pending and subsequent operations to be cancelled
TEST(UVYMQSocketTest, TestRequestSocketStop)
{
    auto result = testRequestStop();
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
    UVYMQTransport,
    UVYMQSocketTest,
    ::testing::ValuesIn(GetTransports()),
    [](const testing::TestParamInfo<UVYMQSocketTest::ParamType>& info) {
        // use tcp/ipc as suffix for test names
        return info.param;
    });

#include <gtest/gtest.h>

#include <cassert>
#include <cstdlib>
#include <limits>
#include <thread>

#include "scaler/io/ymq/bytes.h"
#include "scaler/io/ymq/examples/common.h"
#include "scaler/io/ymq/io_context.h"
#include "tests/cc_ymq/common.h"

// #include "tests/cc_ymq/bad_header.h"
// #include "tests/cc_ymq/basic.h"
// #include "tests/cc_ymq/big_message.h"
// #include "tests/cc_ymq/empty_message.h"
// #include "tests/cc_ymq/incomplete_identity.h"
// #include "tests/cc_ymq/passthrough.h"
// #include "tests/cc_ymq/reconnect.h"
// #include "tests/cc_ymq/slow.h"

using namespace scaler::ymq;
using namespace std::chrono_literals;

TestResult basic_server_main()
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Binder, "server");
    syncBindSocket(socket, "tcp://127.0.0.1:25711");
    auto result = syncRecvMessage(socket);

    RETURN_FAILURE_IF_TRUE(result.has_value());
    RETURN_FAILURE_IF_TRUE(result->payload.as_string() == "yi er san si wu liu");

    context.removeIOSocket(socket);

    return TestResult::Success;
}

TestResult basic_client_main(int delay)
{
    TcpSocket socket;

    socket.connect("127.0.0.1", 25711);

    socket.write_message("client");
    auto remote_identity = socket.read_message();
    RETURN_FAILURE_IF_TRUE(remote_identity == "server");
    socket.write_message("yi er san si wu liu");

    if (delay)
        std::this_thread::sleep_for(std::chrono::seconds(delay));

    return TestResult::Success;
}

TEST(CcYmqTestSuite, TestBasicDelay)
{
    auto result = test(10, {[] { return basic_client_main(5); }, basic_server_main});
    EXPECT_EQ(result, TestResult::Success);
}

// TODO: this should pass
TEST(CcYmqTestSuite, DISABLED_TestBasicNoDelay)
{
    auto result = test(10, {[] { return basic_client_main(0); }, basic_server_main});

    EXPECT_EQ(result, TestResult::Success);
}

TestResult big_message_server_main()
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Binder, "server");
    syncBindSocket(socket, "tcp://127.0.0.1:25711");
    auto result = syncRecvMessage(socket);

    RETURN_FAILURE_IF_TRUE(result.has_value());
    RETURN_FAILURE_IF_TRUE(result->payload.len() == 500'000'000);

    context.removeIOSocket(socket);

    return TestResult::Success;
}

TestResult big_message_client_main(int delay)
{
    TcpSocket socket;

    socket.connect("127.0.0.1", 25711);

    socket.write_message("client");
    auto remote_identity = socket.read_message();
    RETURN_FAILURE_IF_TRUE(remote_identity == "server");
    std::string msg(500'000'000, '.');
    socket.write_message(msg);

    if (delay)
        std::this_thread::sleep_for(std::chrono::seconds(delay));

    return TestResult::Success;
}

TEST(CcYmqTestSuite, TestBigMessage)
{
    auto result = test(10, {[] { return big_message_client_main(5); }, big_message_server_main});
    EXPECT_EQ(result, TestResult::Success);
}

TestResult passthrough_server_main()
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Binder, "server");
    syncBindSocket(socket, "tcp://192.0.2.3:23571");
    auto result = syncRecvMessage(socket);

    RETURN_FAILURE_IF_TRUE(result.has_value());
    RETURN_FAILURE_IF_TRUE(result->payload.as_string() == "yi er san si wu liu");

    context.removeIOSocket(socket);

    return TestResult::Success;
}

TestResult passthrough_client_main(int delay)
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Connector, "client");
    syncConnectSocket(socket, "tcp://192.0.2.4:2323");
    auto result = syncSendMessage(socket, {.address = Bytes("server"), .payload = Bytes("yi er san si wu liu")});

    context.removeIOSocket(socket);

    return TestResult::Success;
}

TEST(CcYmqTestSuite, TestMitmPassthrough)
{
    auto result = test(
        20,
        {[] { return run_mitm(L"passthrough", {L"192.0.2.4", L"2323", L"192.0.2.3", L"23571"}); },
         [] { return passthrough_client_main(3); },
         passthrough_server_main},
        true);
    EXPECT_EQ(result, TestResult::Success);
}

TestResult reconnect_server_main()
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Binder, "server");
    syncBindSocket(socket, "tcp://192.0.2.1:23571");
    auto result = syncRecvMessage(socket);

    RETURN_FAILURE_IF_TRUE(result.has_value());
    RETURN_FAILURE_IF_TRUE(result->payload.as_string() == "hello!!");

    context.removeIOSocket(socket);

    return TestResult::Success;
}

TestResult reconnect_client_main(int delay)
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Connector, "client");
    syncConnectSocket(socket, "tcp://192.0.2.2:2323");
    auto result = syncSendMessage(socket, {.address = Bytes("server"), .payload = Bytes("hello!!")});

    context.removeIOSocket(socket);

    return TestResult::Success;
}

TEST(CcYmqTestSuite, DISABLED_TestMitmReconnect)
{
    auto result = test(
        10,
        {[] { return run_python("tests/cc_ymq/reconnect.py", {L"192.0.2.2", L"2323", L"192.0.2.1", L"23571"}); },
         [] { return reconnect_client_main(3); },
         reconnect_server_main},
        true);
    EXPECT_EQ(result, TestResult::Success);
}

TEST(CcYmqTestSuite, TestMitmDrop)
{
    auto result = test(
        60,
        {[] { return run_mitm(L"drop", {L"192.0.2.4", L"2323", L"192.0.2.3", L"23571", L"0.3"}); },
         [] { return passthrough_client_main(3); },
         passthrough_server_main},
        true);
    EXPECT_EQ(result, TestResult::Success);
}

TestResult slow_server_main()
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Binder, "server");
    syncBindSocket(socket, "tcp://127.0.0.1:25713");
    auto result = syncRecvMessage(socket);

    RETURN_FAILURE_IF_TRUE(result.has_value());
    RETURN_FAILURE_IF_TRUE(result->payload.as_string() == "yi er san si wu liu");

    context.removeIOSocket(socket);

    return TestResult::Success;
}

// TODO: implement this using mitm
TestResult slow_client_main()
{
    TcpSocket socket;

    socket.connect("127.0.0.1", 25713);

    socket.write_message("client");
    auto remote_identity = socket.read_message();
    RETURN_FAILURE_IF_TRUE(remote_identity == "server");

    std::string message = "yi er san si wu liu";
    uint64_t header     = message.length();

    socket.write_all((char*)&header, 4);
    std::this_thread::sleep_for(5s);
    socket.write_all((char*)&header + 4, 4);
    std::this_thread::sleep_for(3s);
    socket.write_all(message.data(), header / 2);
    std::this_thread::sleep_for(5s);
    socket.write_all(message.data() + header / 2, header - header / 2);
    std::this_thread::sleep_for(3s);

    return TestResult::Success;
}

TEST(CcYmqTestSuite, TestSlowNetwork)
{
    auto result = test(20, {slow_client_main, slow_server_main});
    EXPECT_EQ(result, TestResult::Success);
}

TestResult incomplete_identity_server_main()
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Binder, "server");
    syncBindSocket(socket, "tcp://127.0.0.1:25715");
    auto result = syncRecvMessage(socket);

    RETURN_FAILURE_IF_TRUE(result.has_value());
    RETURN_FAILURE_IF_TRUE(result->payload.as_string() == "yi er san si wu liu");

    context.removeIOSocket(socket);

    return TestResult::Success;
}

TestResult incomplete_identity_client_main()
{
    // open a socket, write an incomplete identity and exit
    {
        TcpSocket socket;

        socket.connect("127.0.0.1", 25715);

        auto remote_identity = socket.read_message();
        RETURN_FAILURE_IF_TRUE(remote_identity == "server");

        // write incomplete identity and exit
        std::string identity = "client";
        uint64_t header      = identity.length();
        socket.write_all((char*)&header, 8);
        socket.write_all(identity.data(), identity.length() - 2);
        std::this_thread::sleep_for(3s);
    }

    // connect again and try to send a message
    {
        TcpSocket socket;
        socket.connect("127.0.0.1", 25715);
        auto remote_identity = socket.read_message();
        RETURN_FAILURE_IF_TRUE(remote_identity == "server");
        socket.write_message("client");
        socket.write_message("yi er san si wu liu");
        std::this_thread::sleep_for(3s);
    }

    return TestResult::Success;
}

TEST(CcYmqTestSuite, TestIncompleteIdentity)
{
    auto result = test(20, {incomplete_identity_client_main, incomplete_identity_server_main});
    EXPECT_EQ(result, TestResult::Success);
}

TestResult bad_header_server_main()
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Binder, "server");
    syncBindSocket(socket, "tcp://127.0.0.1:25713");
    auto result = syncRecvMessage(socket);

    RETURN_FAILURE_IF_TRUE(result.has_value());
    RETURN_FAILURE_IF_TRUE(result->payload.as_string() == "yi er san si wu liu");

    context.removeIOSocket(socket);

    return TestResult::Success;
}

TestResult bad_header_client_main()
{
    TcpSocket socket;

    socket.connect("127.0.0.1", 25713);

    socket.write_message("client");
    auto remote_identity = socket.read_message();
    RETURN_FAILURE_IF_TRUE(remote_identity == "server");

    uint64_t header = std::numeric_limits<uint64_t>::max();
    socket.write_all((char*)&header, 8);

    // TODO: this sleep shouldn't be necessary
    std::this_thread::sleep_for(3s);

    return TestResult::Success;
}

// TODO: this should pass
TEST(CcYmqTestSuite, DISABLED_TestBadHeader)
{
    auto result = test(20, {bad_header_client_main, bad_header_server_main});
    EXPECT_EQ(result, TestResult::Success);
}

TestResult empty_message_server_main()
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Binder, "server");
    syncBindSocket(socket, "tcp://127.0.0.1:25713");

    auto result = syncRecvMessage(socket);
    RETURN_FAILURE_IF_TRUE(result.has_value());
    RETURN_FAILURE_IF_TRUE(result->payload.as_string() == "");

    auto result2 = syncRecvMessage(socket);
    RETURN_FAILURE_IF_TRUE(result2.has_value());
    RETURN_FAILURE_IF_TRUE(result2->payload.as_string() == "");

    context.removeIOSocket(socket);

    return TestResult::Success;
}

TestResult empty_message_client_main()
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Connector, "client");
    syncConnectSocket(socket, "tcp://127.0.0.1:25713");

    auto error = syncSendMessage(socket, Message {.address = Bytes(), .payload = Bytes()});
    RETURN_FAILURE_IF_TRUE(!error);

    auto error2 = syncSendMessage(socket, Message {.address = Bytes(), .payload = Bytes("")});
    RETURN_FAILURE_IF_TRUE(!error2);

    context.removeIOSocket(socket);

    return TestResult::Success;
}

TEST(CcYmqTestSuite, TestEmptyMessage)
{
    auto result = test(20, {empty_message_client_main, empty_message_server_main});
    EXPECT_EQ(result, TestResult::Success);
}

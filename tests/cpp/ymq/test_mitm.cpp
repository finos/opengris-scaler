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
#include <filesystem>
#include <format>
#include <string>
#include <thread>

#include "scaler/object_storage/message.h"
#include "scaler/object_storage/object_storage_server.h"
#include "scaler/ymq/bytes.h"
#include "scaler/ymq/future/connector_socket.h"
#include "scaler/ymq/io_context.h"
#include "scaler/ymq/sync/binder_socket.h"
#include "tests/cpp/ymq/common/testing.h"
#include "tests/cpp/ymq/common/utils.h"

using scaler::object_storage::ObjectRequestHeader;
using scaler::object_storage::ObjectStorageServer;
using ObjectRequestType = scaler::protocol::ObjectRequestHeader::ObjectRequestType;

// Helper client/server functions defined in test_sockets.cpp
TestResult basicClientYmq(std::string address);
TestResult basicServerYmq(std::string address);

class YMQMitmTest: public ::testing::Test {};

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
    scaler::ymq::IOContext context {};

    scaler::ymq::sync::BinderSocket socket {context, "server"};
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

    scaler::ymq::IOContext context {};

    auto socketResult = scaler::ymq::future::ConnectorSocket::connect(context, "client", address);
    RETURN_FAILURE_IF_FALSE(socketResult.has_value());

    auto socket = std::move(socketResult.value());

    // create the recv future in advance, this remains active between reconnects
    auto future = socket.recvMessage();

    // send "sync" and wait for "acknowledge" in a loop
    // the mitm will send a RST after the first "sync"
    // the "sync" message will be lost, but ymq should automatically reconnect
    // therefore the next "sync" message should succeed
    // note: the send on the first iteration may fail with an error when the RST fires mid-write;
    // this is expected and the loop retries on timeout
    for (size_t i = 0; i < retryTimes; i++) {
        auto sendFuture                  = socket.sendMessage(scaler::ymq::Bytes {"sync"});
        [[maybe_unused]] auto sendResult = sendFuture.get();

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
// for this test case we use ymq on both the client side and the server side
// the client connects to the mitm, and the mitm connects to the server
// when the mitm receives packets from the client, it forwards it to the server without changing it
// and similarly when it receives packets from the server, it forwards them to the client
//
// the mitm is implemented in Python. we pass the name of the test case, which corresponds to the Python filename,
// and a list of arguments, which are: mitm ip, mitm port, remote ip, remote port
// this defines the address of the mitm, and the addresses that can connect to it
// for more, see the python mitm files
TEST_F(YMQMitmTest, Passthrough)
{
    auto [mitm_ip, remote_ip] = getMitmIPs();
    auto mitm_port            = 23579;
    auto remote_port          = 23571;

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
TEST_F(YMQMitmTest, PassthroughRaw)
{
    auto [mitm_ip, remote_ip] = getMitmIPs();
    auto mitm_port            = 23580;
    auto remote_port          = 23574;

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

// this test uses the mitm to test the reconnect logic of ymq by sending RST packets
TEST_F(YMQMitmTest, Reconnect)
{
    auto [mitm_ip, remote_ip] = getMitmIPs();
    auto mitm_port            = 23581;
    auto remote_port          = 23572;

    auto result = test(
        30,
        {[=] { return runMitm("send_rst_to_client", mitm_ip, mitm_port, remote_ip, remote_port); },
         [=] { return reconnectServerMain(std::format("tcp://{}:{}", remote_ip, remote_port)); },
         [=] { return reconnectClientMain(std::format("tcp://{}:{}", mitm_ip, mitm_port)); }},
        true);

    EXPECT_EQ(result, TestResult::Success);
}

// Regression test for issue #787.
//
// Root cause: MessageConnection::onWriteDone calls callback({}) (silent success) when
// UV_ECONNRESET fires, so the caller believes the write succeeded even though the
// connection was reset.
//
// Without the fix: the ObjectRequestHeader send resolves as success. The client queues
// the payload in _sendPending while the connection is resetting. After reconnect,
// _sendPending is flushed — the server receives the payload as the first application
// message from this identity and calls ObjectRequestHeader::fromBuffer() on raw payload
// bytes, which throws kj::Exception ("Malformed capnp message").
//
// The MITM (send_rst_after_server_identity) injects RST immediately after forwarding
// the server's identity packet. The RST and the identity are written to the TUN device
// in immediate succession, so libuv processes both in the same uv__read loop before
// returning to epoll_wait. By the time the test thread's sendMessage call is dispatched,
// the socket is already in the reset state, so write() returns UV_ECONNRESET and
// onWriteDone fires with the error.
//
// On unfixed code: onWriteDone calls callback({}) — headerSendResult is success → test fails.
// On fixed code:   onWriteDone calls callback(error) — headerSendResult is error → test passes.
TestResult setObjectHeaderDroppedServerMain(std::string address, std::filesystem::path stopServerPath)
{
    ObjectStorageServer server;

    std::thread serverThread([&server, address = std::move(address), stopServerPath]() mutable {
        server.run(std::move(address), "oss-test", "INFO", "%(levelname)s: %(message)s", {}, [stopServerPath] {
            return !std::filesystem::exists(stopServerPath);
        });
    });

    server.waitUntilReady();
    serverThread.join();

    return TestResult::Success;
}

TestResult setObjectHeaderDroppedClientMain(
    std::string address, std::filesystem::path stopServerPath, std::filesystem::path rendezvousPath)
{
    scaler::ymq::IOContext context {};

    auto socketResult = scaler::ymq::future::ConnectorSocket::connect(context, "oss-mitm-client", address);
    RETURN_FAILURE_IF_FALSE(socketResult.has_value());
    auto socket = std::move(socketResult.value());

    ObjectRequestHeader requestHeader {
        .objectID      = {1, 2, 3, 4},
        .payloadLength = 5,
        .requestID     = 42,
        .requestType   = ObjectRequestType::SET_OBJECT,
    };
    auto headerBuf   = requestHeader.toBuffer();
    auto headerBytes = scaler::ymq::Bytes {(char*)headerBuf.asBytes().begin(), headerBuf.asBytes().size()};

    // Wait for the MITM to signal that the RST has been injected. By the time the sentinel
    // file appears, the RST is already in this socket's kernel receive buffer, so the
    // subsequent sendMessage() will deterministically get UV_ECONNRESET.
    while (!std::filesystem::exists(rendezvousPath))
        std::this_thread::sleep_for(std::chrono::milliseconds(10));

    auto headerSendResult = socket.sendMessage(std::move(headerBytes)).get();

    if (headerSendResult.has_value()) {
        // Header appeared to succeed: the bug is present. Send the payload — YMQ will deliver
        // it to the server as the first application message after reconnect. The server calls
        // ObjectRequestHeader::fromBuffer() on raw payload bytes and logs "Malformed capnp message".
        [[maybe_unused]] auto payloadResult = socket.sendMessage(scaler::ymq::Bytes {"hello"}).get();
        // Give the server time to receive and process the payload before we signal it to stop.
        // Without this, the server may not have processed the payload before the stop file appears.
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    // Signal the server subprocess to stop via a file sentinel — stopServer is not shared
    // across fork() boundaries, so a shared_ptr<atomic<bool>> would not work here.
    std::ofstream stopFile {stopServerPath};

    RETURN_FAILURE_IF_FALSE(!headerSendResult.has_value());
    return TestResult::Success;
}

TEST_F(YMQMitmTest, SetObjectHeaderDropped)
{
    auto [mitm_ip, remote_ip] = getMitmIPs();
    auto mitm_port            = 23583;
    auto remote_port          = 23575;

    auto rendezvousPath = std::filesystem::temp_directory_path() / "ymq_mitm_rst_rendezvous";
    auto stopServerPath = std::filesystem::temp_directory_path() / "ymq_mitm_stop_server";
    std::filesystem::remove(rendezvousPath);
    std::filesystem::remove(stopServerPath);

    auto result = test(
        30,
        {[=] {
             return runMitm(
                 "send_rst_after_server_identity",
                 mitm_ip,
                 mitm_port,
                 remote_ip,
                 remote_port,
                 {rendezvousPath.string()});
         },
         [=] {
             return setObjectHeaderDroppedServerMain(
                 std::format("tcp://{}:{}", remote_ip, remote_port), stopServerPath);
         },
         [=] {
             return setObjectHeaderDroppedClientMain(
                 std::format("tcp://{}:{}", mitm_ip, mitm_port), stopServerPath, rendezvousPath);
         }},
        true);

    std::filesystem::remove(rendezvousPath);
    std::filesystem::remove(stopServerPath);
    EXPECT_EQ(result, TestResult::Success);
}

// in this test, the mitm drops a random % of packets arriving from the client and server
TEST_F(YMQMitmTest, RandomlyDropPackets)
{
    auto [mitm_ip, remote_ip] = getMitmIPs();
    auto mitm_port            = 23582;
    auto remote_port          = 23573;

    auto result = test(
        180,
        {[=] { return runMitm("randomly_drop_packets", mitm_ip, mitm_port, remote_ip, remote_port, {"0.3"}); },
         [=] { return basicServerYmq(std::format("tcp://{}:{}", remote_ip, remote_port)); },
         [=] { return basicClientYmq(std::format("tcp://{}:{}", mitm_ip, mitm_port)); }},
        true);

    EXPECT_EQ(result, TestResult::Success);
}

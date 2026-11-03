#include <gtest/gtest.h>

#include <chrono>
#include <expected>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "scaler/wrapper/uv/error.h"
#include "scaler/wrapper/uv/loop.h"
#include "scaler/ymq/address.h"
#include "scaler/ymq/buffered_bytes.h"
#include "scaler/ymq/bytes.h"
#include "scaler/ymq/connector_socket.h"
#include "scaler/ymq/internal/accept_server.h"
#include "scaler/ymq/internal/message_connection.h"
#include "scaler/ymq/io_context.h"
#include "scaler/ymq/sync/connector_socket.h"
#include "tests/cpp/ymq/common/utils.h"

namespace {

const std::string messagePayload = "Hello from ConnectorSocket!";

// More than loopback's socket buffers hold, so a send to a peer that does not read stays in flight.
constexpr size_t stalledSendBytes = 64 * 1024 * 1024;
constexpr std::chrono::milliseconds sendSettleTime {200};
constexpr std::chrono::seconds teardownTimeout {10};

}  // namespace

// Helper class to set up a server MessageConnection and ConnectorSocket pair
class ConnectorServerPair {
public:
    static const scaler::ymq::Identity serverIdentity;
    static const scaler::ymq::Identity connectorIdentity;

    ConnectorServerPair(
        const std::string& transport,
        scaler::ymq::internal::MessageConnection::RemoteIdentityCallback serverOnIdentity,
        scaler::ymq::internal::MessageConnection::RemoteDisconnectCallback serverOnDisconnect,
        scaler::ymq::internal::MessageConnection::RecvMessageCallback serverOnMessage,
        scaler::ymq::ConnectorSocket::ConnectCallback connectorOnConnect)
        : _context()
        , _loop(UV_EXIT_ON_ERROR(scaler::wrapper::uv::Loop::init()))
        , _serverConnection(
              serverIdentity,
              std::nullopt,
              std::move(serverOnIdentity),
              std::move(serverOnDisconnect),
              std::move(serverOnMessage))
    {
        const auto listenAddress =
            scaler::ymq::Address::fromString(getTransportAddress(transport, 0), getTLSConfig(transport)).value();

        _server = scaler::ymq::internal::AcceptServer::init(
                      _loop,
                      listenAddress,
                      std::bind_front(&scaler::ymq::internal::MessageConnection::connect, &_serverConnection))
                      .value();

        std::string address = _server->address().toString().value();

        _connector = std::make_unique<scaler::ymq::ConnectorSocket>(
            scaler::ymq::ConnectorSocket::connect(_context, connectorIdentity, address, std::move(connectorOnConnect)));
    }

    scaler::ymq::internal::MessageConnection& server()
    {
        return _serverConnection;
    }
    scaler::ymq::ConnectorSocket& connector()
    {
        return *_connector;
    }
    scaler::wrapper::uv::Loop& loop()
    {
        return _loop;
    }

private:
    scaler::ymq::IOContext _context;
    scaler::wrapper::uv::Loop _loop;
    std::optional<scaler::ymq::internal::AcceptServer> _server;
    scaler::ymq::internal::MessageConnection _serverConnection;
    std::unique_ptr<scaler::ymq::ConnectorSocket> _connector;
};

const scaler::ymq::Identity ConnectorServerPair::serverIdentity    = "server-identity";
const scaler::ymq::Identity ConnectorServerPair::connectorIdentity = "connector-identity";

class YMQConnectorSocketTest: public ::testing::TestWithParam<std::string> {};

TEST_P(YMQConnectorSocketTest, ConnectionFailure)
{
    // Test that ConnectorSocket properly handles connection failure

    constexpr int maxRetryTimes = 3;
    constexpr std::chrono::milliseconds initRetryDelay {10};

    scaler::ymq::IOContext context {};

    // Port 49151 is IANA reserved, hopefully never assigned
    auto result = scaler::ymq::sync::ConnectorSocket::connect(
        context,
        ConnectorServerPair::connectorIdentity,
        getTransportAddress(GetParam(), 49151),
        getTLSConfig(GetParam()),
        maxRetryTimes,
        initRetryDelay);

    // Connection should fail after retries
    ASSERT_FALSE(result.has_value());
    ASSERT_EQ(result.error()._errorCode, scaler::ymq::Error::ErrorCode::ConnectorSocketClosedByRemoteEnd);
}

TEST_F(YMQConnectorSocketTest, InvalidAddress)
{
    // Test that ConnectorSocket properly handles invalid address format

    scaler::ymq::IOContext context {};

    auto result =
        scaler::ymq::sync::ConnectorSocket::connect(context, ConnectorServerPair::connectorIdentity, "invalid-address");

    // Connection should fail immediately
    ASSERT_FALSE(result.has_value());
    ASSERT_EQ(result.error()._errorCode, scaler::ymq::Error::ErrorCode::InvalidAddressFormat);
}

TEST_P(YMQConnectorSocketTest, SendMessage)
{
    // Test sending messages before connection, during connection, and after disconnect
    std::promise<void> connectCalled {};

    int serverMessagesReceived = 0;

    ConnectorServerPair connections(
        GetParam(),

        // Server callbacks
        []([[maybe_unused]] auto identity) {},                                       // onRemoteIdentity
        [](auto) { FAIL() << "Unexpected disconnect on server"; },                   // onRemoteDisconnect
        [&]([[maybe_unused]] std::unique_ptr<scaler::ymq::Bytes> receivedPayload) {  // onMessage
            serverMessagesReceived++;
        },

        // Connector callback
        [&](std::expected<void, scaler::ymq::Error> result) {
            ASSERT_TRUE(result.has_value());
            connectCalled.set_value();
        });

    scaler::ymq::internal::MessageConnection& server = connections.server();
    scaler::ymq::ConnectorSocket& connector          = connections.connector();
    scaler::wrapper::uv::Loop& loop                  = connections.loop();

    std::promise<void> sendCallbackCalled {};

    auto onMessageSent = [&](std::expected<void, scaler::ymq::Error> result,
                             [[maybe_unused]] std::unique_ptr<scaler::ymq::Bytes>) {
        ASSERT_TRUE(result.has_value());
        sendCallbackCalled.set_value();
    };

    // Send message BEFORE connection completes
    connector.sendMessage(std::make_unique<scaler::ymq::BufferedBytes>(messagePayload), onMessageSent);

    // Wait for connection to complete
    while (!server.established()) {
        loop.run(UV_RUN_ONCE);
    }
    connectCalled.get_future().get();

    // Wait for first message to be sent
    ASSERT_EQ(sendCallbackCalled.get_future().wait_for(std::chrono::seconds {5}), std::future_status::ready);

    // Send message DURING connection
    sendCallbackCalled = {};
    connector.sendMessage(std::make_unique<scaler::ymq::BufferedBytes>(messagePayload), onMessageSent);

    // Wait for second message to be sent
    ASSERT_EQ(sendCallbackCalled.get_future().wait_for(std::chrono::seconds {5}), std::future_status::ready);

    // Wait for both messages to be received
    while (serverMessagesReceived < 2) {
        loop.run(UV_RUN_ONCE);
    }

    // Disconnect from the server side
    server.disconnect();

    loop.run(UV_RUN_ONCE);

    // Give some time for the disconnect to propagate
    std::this_thread::sleep_for(std::chrono::milliseconds {100});

    // Try to send a message AFTER disconnection
    std::promise<scaler::ymq::Error> sendErrorReceived {};

    auto onMessageSentError = [&](std::expected<void, scaler::ymq::Error> result,
                                  [[maybe_unused]] std::unique_ptr<scaler::ymq::Bytes>) {
        ASSERT_FALSE(result.has_value());
        sendErrorReceived.set_value(result.error());
    };

    connector.sendMessage(std::make_unique<scaler::ymq::BufferedBytes>(messagePayload), onMessageSentError);

    // Wait for send to fail
    scaler::ymq::Error error = sendErrorReceived.get_future().get();
    ASSERT_EQ(error._errorCode, scaler::ymq::Error::ErrorCode::ConnectorSocketClosedByRemoteEnd);
}

TEST_P(YMQConnectorSocketTest, RecvMessage)
{
    // Test receiving messages before and after connection

    std::promise<void> connectCalled {};

    ConnectorServerPair connections(
        GetParam(),

        // Server callbacks
        []([[maybe_unused]] auto identity) {},                      // onRemoteIdentity
        [](auto) { FAIL() << "Unexpected disconnect on server"; },  // onRemoteDisconnect
        [](auto) { FAIL() << "Unexpected message on server"; },     // onMessage

        // Connector callback
        [&](std::expected<void, scaler::ymq::Error> result) {
            ASSERT_TRUE(result.has_value());
            connectCalled.set_value();
        });

    scaler::ymq::internal::MessageConnection& server = connections.server();
    scaler::ymq::ConnectorSocket& connector          = connections.connector();
    scaler::wrapper::uv::Loop& loop                  = connections.loop();

    std::promise<scaler::ymq::Message> recvCalled {};

    auto onConnectorRecvMessage = [&](std::expected<scaler::ymq::Message, scaler::ymq::Error> result) {
        ASSERT_TRUE(result.has_value());
        recvCalled.set_value(std::move(*result));
    };

    // Register receive callback BEFORE connection completes
    connector.recvMessage(onConnectorRecvMessage);

    // Wait for connection to complete
    while (!server.established()) {
        loop.run(UV_RUN_ONCE);
    }
    connectCalled.get_future().get();

    // Send first message from server
    bool sendCalled    = false;
    auto onMessageSent = [&](std::expected<void, scaler::ymq::Error> result,
                             [[maybe_unused]] std::unique_ptr<scaler::ymq::Bytes>) {
        ASSERT_TRUE(result.has_value());
        sendCalled = true;
    };

    server.sendMessage(std::make_unique<scaler::ymq::BufferedBytes>(messagePayload), onMessageSent);

    // Wait for message to be sent
    while (!sendCalled) {
        loop.run(UV_RUN_ONCE);
    }

    // Wait for first message to be received
    scaler::ymq::Message message = recvCalled.get_future().get();
    ASSERT_EQ(message.address->asString(), ConnectorServerPair::serverIdentity);
    ASSERT_EQ(message.payload->asString(), messagePayload);

    // Register receive callback AFTER connection is established
    recvCalled = {};
    connector.recvMessage(onConnectorRecvMessage);

    // Send second message from server
    sendCalled = false;
    server.sendMessage(std::make_unique<scaler::ymq::BufferedBytes>(messagePayload), onMessageSent);

    // Wait for message to be sent
    while (!sendCalled) {
        loop.run(UV_RUN_ONCE);
    }

    // Wait for second message to be received
    message = recvCalled.get_future().get();
}

TEST_P(YMQConnectorSocketTest, RemoteDisconnect)
{
    // Test that ConnectorSocket properly handles a graceful remote disconnection

    std::promise<void> connectCalled {};

    ConnectorServerPair connections(
        GetParam(),

        // Server callbacks
        []([[maybe_unused]] auto identity) {},                      // onRemoteIdentity
        [](auto) { FAIL() << "Unexpected disconnect on server"; },  // onRemoteDisconnect
        [](auto) { FAIL() << "Unexpected message on server"; },     // onMessage

        // Connector callback
        [&](std::expected<void, scaler::ymq::Error> result) {
            ASSERT_TRUE(result.has_value());
            connectCalled.set_value();
        });

    scaler::ymq::internal::MessageConnection& server = connections.server();
    scaler::ymq::ConnectorSocket& connector          = connections.connector();
    scaler::wrapper::uv::Loop& loop                  = connections.loop();

    // Wait for connection to complete
    while (!server.established()) {
        loop.run(UV_RUN_ONCE);
    }
    connectCalled.get_future().get();

    // Register a receive callback
    std::promise<scaler::ymq::Error> recvCalled {};

    auto onConnectorRecvMessage = [&](std::expected<scaler::ymq::Message, scaler::ymq::Error> result) {
        ASSERT_FALSE(result.has_value());
        recvCalled.set_value(result.error());
    };

    connector.recvMessage(onConnectorRecvMessage);

    // Gracefully disconnect from the server side
    server.disconnect();
    loop.run(UV_RUN_ONCE);

    // Wait for the receive callback to be called with an error
    scaler::ymq::Error error = recvCalled.get_future().get();
    ASSERT_EQ(error._errorCode, scaler::ymq::Error::ErrorCode::ConnectorSocketClosedByRemoteEnd);
}

TEST_P(YMQConnectorSocketTest, Reconnect)
{
    // Test that ConnectorSocket automatically reconnects after an unexpected disconnection (abort)

    ConnectorServerPair connections(
        GetParam(),

        // Server callbacks
        []([[maybe_unused]] auto identity) {},                   // onRemoteIdentity
        []([[maybe_unused]] auto reason) {},                     // onRemoteDisconnect
        [](auto) { FAIL() << "Unexpected message on server"; },  // onMessage

        // Connector callback
        []([[maybe_unused]] auto result) {});

    scaler::ymq::internal::MessageConnection& server = connections.server();
    scaler::ymq::ConnectorSocket& connector          = connections.connector();
    scaler::wrapper::uv::Loop& loop                  = connections.loop();

    // Send a message from the server to the client.
    // Required as Linux might not send a RST segment if both connections are not fully initialized.

    std::promise<void> messageReceived {};
    auto onConnectorRecvMessage = [&](auto) { messageReceived.set_value(); };

    connector.recvMessage(onConnectorRecvMessage);

    bool sendCalled = false;
    server.sendMessage(
        std::make_unique<scaler::ymq::BufferedBytes>(messagePayload), [&](auto, auto) { sendCalled = true; });

    while (!sendCalled) {
        loop.run(UV_RUN_ONCE);
    }

    ASSERT_EQ(messageReceived.get_future().wait_for(std::chrono::seconds {5}), std::future_status::ready);

    // Abort the connection (simulates unexpected network error)

    server.abort();
    ASSERT_FALSE(server.connected());

    // Wait for the connector to re-establish the connection to the server

    while (!server.established()) {
        loop.run(UV_RUN_ONCE);
    }

    ASSERT_TRUE(server.established());
}

TEST_P(YMQConnectorSocketTest, TeardownWithStalledPeer)
{
    // A peer that stopped reading keeps a stopped socket's graceful shutdown waiting on its writes. Destroying the
    // IOContext must cancel them rather than wait for a peer that may never read again.

    scaler::wrapper::uv::Loop loop = UV_EXIT_ON_ERROR(scaler::wrapper::uv::Loop::init());

    scaler::ymq::internal::MessageConnection stalledPeer(
        "stalled-peer",
        std::nullopt,
        []([[maybe_unused]] auto identity) {},
        []([[maybe_unused]] auto reason) {},
        []([[maybe_unused]] auto message) {});

    const auto listenAddress =
        scaler::ymq::Address::fromString(getTransportAddress(GetParam(), 0), getTLSConfig(GetParam())).value();

    auto server =
        scaler::ymq::internal::AcceptServer::init(
            loop, listenAddress, std::bind_front(&scaler::ymq::internal::MessageConnection::connect, &stalledPeer))
            .value();

    auto context   = std::make_unique<scaler::ymq::IOContext>();
    auto connector = std::make_unique<scaler::ymq::ConnectorSocket>(scaler::ymq::ConnectorSocket::connect(
        *context, "connector", server.address().toString().value(), []([[maybe_unused]] auto result) {}));

    while (!stalledPeer.established()) {
        loop.run(UV_RUN_ONCE);
    }

    // The peer's loop does not run from here on, so it stops reading and the send stays in flight.

    auto sendDone   = std::make_shared<std::promise<std::expected<void, scaler::ymq::Error>>>();
    auto sendResult = sendDone->get_future();
    connector->sendMessage(
        std::make_unique<scaler::ymq::BufferedBytes>(std::string(stalledSendBytes, 'x')),
        [sendDone](auto result, [[maybe_unused]] auto payload) { sendDone->set_value(std::move(result)); });

    // SecureSocket acknowledges a write once queued on its transport, so TLS only pins that the teardown returns.
    const bool sendStaysInFlight = GetParam() != "tls" && GetParam() != "wss";

    if (sendStaysInFlight) {
        ASSERT_EQ(sendResult.wait_for(sendSettleTime), std::future_status::timeout);
    }

    connector.reset();

    // Detached: a teardown that hangs must fail this test, not block it forever.
    auto contextDestroyed       = std::make_shared<std::promise<void>>();
    auto contextDestroyedResult = contextDestroyed->get_future();
    std::thread([context = std::move(context), contextDestroyed]() mutable {
        context.reset();
        contextDestroyed->set_value();
    }).detach();

    ASSERT_EQ(contextDestroyedResult.wait_for(teardownTimeout), std::future_status::ready);

    if (sendStaysInFlight) {
        ASSERT_EQ(sendResult.wait_for(std::chrono::seconds {0}), std::future_status::ready);
        const auto result = sendResult.get();
        ASSERT_FALSE(result.has_value());
        ASSERT_EQ(result.error()._errorCode, scaler::ymq::Error::ErrorCode::SocketStopRequested);
    }
}

TEST_P(YMQConnectorSocketTest, Bind)
{
    // Test that a connecting ConnectorSocket can connect and exchange with a binding ConnectorSocket

    scaler::ymq::IOContext context {};

    const scaler::ymq::Identity binderIdentity    = "binder-identity";
    const scaler::ymq::Identity connectorIdentity = "connector-identity";

    // Create a binding connector socket
    auto binderResult = scaler::ymq::sync::ConnectorSocket::bind(
        context, binderIdentity, getTransportAddress(GetParam(), 0), getTLSConfig(GetParam()));
    ASSERT_TRUE(binderResult.has_value());
    auto [binderSocket, boundAddress] = std::move(binderResult.value());

    // Create a connecting connector socket
    auto connectorResult = scaler::ymq::sync::ConnectorSocket::connect(
        context, connectorIdentity, boundAddress.toString().value(), getTLSConfig(GetParam()));
    ASSERT_TRUE(connectorResult.has_value());
    auto connectorSocket = std::move(connectorResult.value());

    // Send a message from the connecting connector
    auto sendResult1 = connectorSocket.sendMessage(std::make_unique<scaler::ymq::BufferedBytes>(messagePayload));
    ASSERT_TRUE(sendResult1.has_value());

    // Receive the message on binding connector
    auto recvResult1 = binderSocket.recvMessage();
    ASSERT_TRUE(recvResult1.has_value());
    ASSERT_EQ(recvResult1.value().address->asString(), connectorIdentity);
    ASSERT_EQ(recvResult1.value().payload->asString(), messagePayload);

    // Send a message from the binding connector
    auto sendResult2 = binderSocket.sendMessage(std::make_unique<scaler::ymq::BufferedBytes>(messagePayload));
    ASSERT_TRUE(sendResult2.has_value());

    // Receive the message on the connecting connector
    auto recvResult2 = connectorSocket.recvMessage();
    ASSERT_TRUE(recvResult2.has_value());
    ASSERT_EQ(recvResult2.value().address->asString(), binderIdentity);
    ASSERT_EQ(recvResult2.value().payload->asString(), messagePayload);
}

INSTANTIATE_TEST_SUITE_P(
    YMQTransport,
    YMQConnectorSocketTest,
    ::testing::ValuesIn(getTransports()),
    [](const testing::TestParamInfo<YMQConnectorSocketTest::ParamType>& info) { return info.param; });

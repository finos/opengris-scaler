#include <gtest/gtest.h>

#include <chrono>
#include <expected>
#include <future>
#include <string>

#include "scaler/uv_ymq/address.h"
#include "scaler/uv_ymq/connector_socket.h"
#include "scaler/uv_ymq/internal/message_connection.h"
#include "scaler/uv_ymq/io_context.h"
#include "scaler/wrapper/uv/error.h"
#include "scaler/wrapper/uv/loop.h"
#include "scaler/wrapper/uv/tcp.h"
#include "scaler/ymq/bytes.h"

namespace {

const std::string messagePayload = "Hello from ConnectorSocket!";

}  // namespace

// Helper class to set up a server MessageConnection and ConnectorSocket pair
class ConnectorServerPair {
public:
    static const scaler::uv_ymq::Identity serverIdentity;
    static const scaler::uv_ymq::Identity connectorIdentity;

    ConnectorServerPair(
        scaler::uv_ymq::internal::MessageConnection::RemoteIdentityCallback serverOnIdentity,
        scaler::uv_ymq::internal::MessageConnection::RemoteDisconnectCallback serverOnDisconnect,
        scaler::uv_ymq::internal::MessageConnection::RecvMessageCallback serverOnMessage,
        scaler::uv_ymq::ConnectorSocket::ConnectCallback connectorOnConnect)
        : _context()
        , _loop(UV_EXIT_ON_ERROR(scaler::wrapper::uv::Loop::init()))
        , _server(UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPServer::init(_loop)))
        , _serverConnection(
              _loop,
              serverIdentity,
              std::nullopt,
              std::move(serverOnIdentity),
              std::move(serverOnDisconnect),
              std::move(serverOnMessage))
    {
        const auto listenAddress = scaler::uv_ymq::Address::fromString("tcp://127.0.0.1:0").value();
        UV_EXIT_ON_ERROR(_server.bind(listenAddress.asTCP(), uv_tcp_flags(0)));

        UV_EXIT_ON_ERROR(_server.listen(16, [&](std::expected<void, scaler::wrapper::uv::Error>) {
            scaler::wrapper::uv::TCPSocket serverSocket = UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPSocket::init(_loop));
            UV_EXIT_ON_ERROR(_server.accept(serverSocket));

            _serverConnection.connect(std::move(serverSocket));
        }));

        scaler::wrapper::uv::SocketAddress serverAddr = UV_EXIT_ON_ERROR(_server.getSockName());
        std::string address                           = "tcp://127.0.0.1:" + std::to_string(serverAddr.port());

        _connector = std::make_unique<scaler::uv_ymq::ConnectorSocket>(
            _context, connectorIdentity, address, std::move(connectorOnConnect));
    }

    scaler::uv_ymq::internal::MessageConnection& server() { return _serverConnection; }
    scaler::uv_ymq::ConnectorSocket& connector() { return *_connector; }
    scaler::wrapper::uv::Loop& loop() { return _loop; }

private:
    scaler::uv_ymq::IOContext _context;
    scaler::wrapper::uv::Loop _loop;
    scaler::wrapper::uv::TCPServer _server;
    scaler::uv_ymq::internal::MessageConnection _serverConnection;
    std::unique_ptr<scaler::uv_ymq::ConnectorSocket> _connector;
};

const scaler::uv_ymq::Identity ConnectorServerPair::serverIdentity    = "server-identity";
const scaler::uv_ymq::Identity ConnectorServerPair::connectorIdentity = "connector-identity";

class UVYMQConnectorSocketTest: public ::testing::Test {};

TEST_F(UVYMQConnectorSocketTest, ConnectionFailure)
{
    // Test that ConnectorSocket properly handles connection failure

    constexpr int maxRetryTimes = 3;
    constexpr std::chrono::milliseconds initRetryDelay {10};

    scaler::uv_ymq::IOContext context {};

    std::promise<scaler::ymq::Error> connectCalled {};

    auto onConnectCallback = [&](std::expected<void, scaler::ymq::Error> result) {
        ASSERT_FALSE(result.has_value());
        connectCalled.set_value(result.error());
    };

    // Port 49151 is IANA reserved, hopefully never assigned
    scaler::uv_ymq::ConnectorSocket connector {
        context,
        ConnectorServerPair::connectorIdentity,
        "tcp://127.0.0.1:49151",
        std::move(onConnectCallback),
        maxRetryTimes,
        initRetryDelay};

    // Wait for connection to fail after retries
    scaler::ymq::Error error = connectCalled.get_future().get();
    ASSERT_EQ(error._errorCode, scaler::ymq::Error::ErrorCode::ConnectorSocketClosedByRemoteEnd);
}

TEST_F(UVYMQConnectorSocketTest, InvalidAddress)
{
    // Test that ConnectorSocket properly handles invalid address format

    scaler::uv_ymq::IOContext context {};

    std::promise<scaler::ymq::Error> connectCalled {};

    auto onConnectCallback = [&](std::expected<void, scaler::ymq::Error> result) {
        ASSERT_FALSE(result.has_value());
        connectCalled.set_value(result.error());
    };

    scaler::uv_ymq::ConnectorSocket connector {
        context, ConnectorServerPair::connectorIdentity, "invalid-address", std::move(onConnectCallback)};

    // Wait for connection to fail immediately
    scaler::ymq::Error error = connectCalled.get_future().get();
    ASSERT_EQ(error._errorCode, scaler::ymq::Error::ErrorCode::InvalidAddressFormat);
}

TEST_F(UVYMQConnectorSocketTest, SendMessage)
{
    // Test sending messages before connection, during connection, and after disconnect

    std::promise<void> connectCalled {};

    int serverMessagesReceived = 0;

    ConnectorServerPair connections(
        // Server callbacks
        [](auto identity) {},                                       // onRemoteIdentity
        [](auto) { FAIL() << "Unexpected disconnect on server"; },  // onRemoteDisconnect
        [&](scaler::ymq::Bytes receivedPayload) {                   // onMessage
            serverMessagesReceived++;
        },

        // Connector callback
        [&](std::expected<void, scaler::ymq::Error> result) {
            ASSERT_TRUE(result.has_value());
            connectCalled.set_value();
        });

    scaler::uv_ymq::internal::MessageConnection& server = connections.server();
    scaler::uv_ymq::ConnectorSocket& connector          = connections.connector();
    scaler::wrapper::uv::Loop& loop                     = connections.loop();

    std::promise<void> sendCallbackCalled {};

    auto onMessageSent = [&](std::expected<void, scaler::ymq::Error> result) {
        ASSERT_TRUE(result.has_value());
        sendCallbackCalled.set_value();
    };

    // Send message BEFORE connection completes
    connector.sendMessage(scaler::ymq::Bytes(messagePayload), onMessageSent);

    // Wait for connection to complete
    connectCalled.get_future().get();

    // Wait for first message to be sent
    ASSERT_EQ(sendCallbackCalled.get_future().wait_for(std::chrono::seconds {5}), std::future_status::ready);

    // Send message DURING connection
    sendCallbackCalled = {};
    connector.sendMessage(scaler::ymq::Bytes(messagePayload), onMessageSent);

    // Wait for second message to be sent
    ASSERT_EQ(sendCallbackCalled.get_future().wait_for(std::chrono::seconds {5}), std::future_status::ready);

    // Wait for both messages to be received
    while (serverMessagesReceived < 2) {
        loop.run(UV_RUN_ONCE);
    }

    // Disconnect from the server side
    server.disconnect();

    // Give some time for the disconnect to propagate
    std::this_thread::sleep_for(std::chrono::milliseconds {100});

    // Try to send a message AFTER disconnection
    std::promise<scaler::ymq::Error> sendErrorReceived {};

    auto onMessageSentError = [&](std::expected<void, scaler::ymq::Error> result) {
        ASSERT_FALSE(result.has_value());
        sendErrorReceived.set_value(result.error());
    };

    connector.sendMessage(scaler::ymq::Bytes(messagePayload), onMessageSentError);

    // Wait for send to fail
    scaler::ymq::Error error = sendErrorReceived.get_future().get();
    ASSERT_EQ(error._errorCode, scaler::ymq::Error::ErrorCode::ConnectorSocketClosedByRemoteEnd);
}

TEST_F(UVYMQConnectorSocketTest, RecvMessage)
{
    // Test receiving messages before and after connection

    std::promise<void> connectCalled {};

    ConnectorServerPair connections(
        // Server callbacks
        [](auto identity) {},                                       // onRemoteIdentity
        [](auto) { FAIL() << "Unexpected disconnect on server"; },  // onRemoteDisconnect
        [](auto) { FAIL() << "Unexpected message on server"; },     // onMessage

        // Connector callback
        [&](std::expected<void, scaler::ymq::Error> result) {
            ASSERT_TRUE(result.has_value());
            connectCalled.set_value();
        });

    scaler::uv_ymq::internal::MessageConnection& server = connections.server();
    scaler::uv_ymq::ConnectorSocket& connector          = connections.connector();
    scaler::wrapper::uv::Loop& loop                     = connections.loop();

    std::promise<scaler::ymq::Message> recvCalled {};

    auto onConnectorRecvMessage = [&](std::expected<scaler::ymq::Message, scaler::ymq::Error> result) {
        ASSERT_TRUE(result.has_value());
        recvCalled.set_value(result.value());
    };

    // Register receive callback BEFORE connection completes
    connector.recvMessage(onConnectorRecvMessage);

    // Wait for connection to complete
    connectCalled.get_future().get();

    // Wait for identity exchange
    while (!server.established()) {
        loop.run(UV_RUN_ONCE);
    }

    // Send first message from server
    bool sendCalled    = false;
    auto onMessageSent = [&](std::expected<void, scaler::ymq::Error> result) {
        ASSERT_TRUE(result.has_value());
        sendCalled = true;
    };

    server.sendMessage(scaler::ymq::Bytes(messagePayload), onMessageSent);

    // Wait for message to be sent
    while (!sendCalled) {
        loop.run(UV_RUN_ONCE);
    }

    // Wait for first message to be received
    scaler::ymq::Message message = recvCalled.get_future().get();
    ASSERT_EQ(message.address.as_string(), ConnectorServerPair::serverIdentity);
    ASSERT_EQ(message.payload.as_string(), messagePayload);

    // Register receive callback AFTER connection is established
    recvCalled = {};
    connector.recvMessage(onConnectorRecvMessage);

    // Send second message from server
    sendCalled = false;
    server.sendMessage(scaler::ymq::Bytes(messagePayload), onMessageSent);

    // Wait for message to be sent
    while (!sendCalled) {
        loop.run(UV_RUN_ONCE);
    }

    // Wait for second message to be received
    message = recvCalled.get_future().get();
}

TEST_F(UVYMQConnectorSocketTest, RemoteDisconnect)
{
    // Test that ConnectorSocket properly handles a graceful remote disconnection

    std::promise<void> connectCalled {};

    ConnectorServerPair connections(
        // Server callbacks
        [](auto identity) {},                                       // onRemoteIdentity
        [](auto) { FAIL() << "Unexpected disconnect on server"; },  // onRemoteDisconnect
        [](auto) { FAIL() << "Unexpected message on server"; },     // onMessage

        // Connector callback
        [&](std::expected<void, scaler::ymq::Error> result) {
            ASSERT_TRUE(result.has_value());
            connectCalled.set_value();
        });

    scaler::uv_ymq::internal::MessageConnection& server = connections.server();
    scaler::uv_ymq::ConnectorSocket& connector          = connections.connector();
    scaler::wrapper::uv::Loop& loop                     = connections.loop();

    // Wait for connection to complete
    connectCalled.get_future().get();

    // Wait for identity exchange
    while (!server.established()) {
        loop.run(UV_RUN_ONCE);
    }

    // Register a receive callback
    std::promise<scaler::ymq::Error> recvCalled {};

    auto onConnectorRecvMessage = [&](std::expected<scaler::ymq::Message, scaler::ymq::Error> result) {
        ASSERT_FALSE(result.has_value());
        recvCalled.set_value(result.error());
    };

    connector.recvMessage(onConnectorRecvMessage);

    // Gracefully disconnect from the server side
    server.disconnect();

    // Wait for the receive callback to be called with an error
    scaler::ymq::Error error = recvCalled.get_future().get();
    ASSERT_EQ(error._errorCode, scaler::ymq::Error::ErrorCode::ConnectorSocketClosedByRemoteEnd);
}

TEST_F(UVYMQConnectorSocketTest, Reconnect)
{
    // Test that ConnectorSocket automatically reconnects after an unexpected disconnection (abort)

    ConnectorServerPair connections(
        // Server callbacks
        [](auto identity) {},                                    // onRemoteIdentity
        [](auto reason) {},                                      // onRemoteDisconnect
        [](auto) { FAIL() << "Unexpected message on server"; },  // onMessage

        // Connector callback
        [](auto result) {});

    scaler::uv_ymq::internal::MessageConnection& server = connections.server();
    scaler::uv_ymq::ConnectorSocket& connector          = connections.connector();
    scaler::wrapper::uv::Loop& loop                     = connections.loop();

    // Send a message from the server to the client.
    // Required as Linux might not send a RST segment if both connections are not fully initialized.

    std::promise<void> messageReceived {};
    auto onConnectorRecvMessage = [&](auto) { messageReceived.set_value(); };

    connector.recvMessage(onConnectorRecvMessage);

    bool sendCalled = false;
    server.sendMessage(scaler::ymq::Bytes(messagePayload), [&](auto) { sendCalled = true; });

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

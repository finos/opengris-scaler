#include <gtest/gtest.h>

#include <expected>
#include <string>

#include "scaler/wrapper/uv/callback.h"
#include "scaler/wrapper/uv/error.h"
#include "scaler/wrapper/uv/loop.h"
#include "scaler/wrapper/uv/tcp.h"
#include "scaler/ymq/address.h"
#include "scaler/ymq/bytes.h"
#include "scaler/ymq/internal/message_connection.h"

class YMQMessageConnectionTest: public ::testing::Test {};

// Helper class to set up a server and client message connection pair
class ConnectionPair {
public:
    static const scaler::ymq::Identity serverIdentity;
    static const scaler::ymq::Identity clientIdentity;

    ConnectionPair(
        scaler::ymq::internal::MessageConnection::RemoteIdentityCallback serverOnIdentity,
        scaler::ymq::internal::MessageConnection::RemoteDisconnectCallback serverOnDisconnect,
        scaler::ymq::internal::MessageConnection::RecvMessageCallback serverOnMessage,
        scaler::ymq::internal::MessageConnection::RemoteIdentityCallback clientOnIdentity,
        scaler::ymq::internal::MessageConnection::RemoteDisconnectCallback clientOnDisconnect,
        scaler::ymq::internal::MessageConnection::RecvMessageCallback clientOnMessage)
        : _loop(UV_EXIT_ON_ERROR(scaler::wrapper::uv::Loop::init()))
        , _server(UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPServer::init(_loop)))
        , _serverConnection(
              serverIdentity,
              std::nullopt,
              std::move(serverOnIdentity),
              std::move(serverOnDisconnect),
              std::move(serverOnMessage))
        , _clientSocket(UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPSocket::init(_loop)))
        , _clientConnection(
              clientIdentity,
              std::nullopt,
              std::move(clientOnIdentity),
              std::move(clientOnDisconnect),
              std::move(clientOnMessage))
    {
        const auto listenAddress = scaler::ymq::Address::fromString("tcp://127.0.0.1:0").value();
        UV_EXIT_ON_ERROR(_server.bind(listenAddress.asTCP(), uv_tcp_flags(0)));

        UV_EXIT_ON_ERROR(_server.listen(16, [&](std::expected<void, scaler::wrapper::uv::Error>) {
            scaler::wrapper::uv::TCPSocket serverSocket = UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPSocket::init(_loop));
            UV_EXIT_ON_ERROR(_server.accept(serverSocket));

            _serverConnection.connect(std::move(serverSocket));
        }));

        UV_EXIT_ON_ERROR(
            _clientSocket.connect(serverAddress(), [this](std::expected<void, scaler::wrapper::uv::Error>) {
                _clientConnection.connect(std::move(_clientSocket));
            }));
    }

    scaler::wrapper::uv::SocketAddress serverAddress() const { return UV_EXIT_ON_ERROR(_server.getSockName()); }

    scaler::wrapper::uv::Loop& loop() { return _loop; }
    scaler::ymq::internal::MessageConnection& server() { return _serverConnection; }
    scaler::ymq::internal::MessageConnection& client() { return _clientConnection; }

private:
    scaler::wrapper::uv::Loop _loop;
    scaler::wrapper::uv::TCPServer _server;
    scaler::ymq::internal::MessageConnection _serverConnection;

    scaler::wrapper::uv::TCPSocket _clientSocket;
    scaler::ymq::internal::MessageConnection _clientConnection;
};

const scaler::ymq::Identity ConnectionPair::serverIdentity = "server-identity";
const scaler::ymq::Identity ConnectionPair::clientIdentity = "client-identity";

TEST_F(YMQMessageConnectionTest, IdentityExchange)
{
    // Test that two MessageConnections successfully exchange identities

    ConnectionPair connections(
        // Server callbacks
        [](auto identity) { ASSERT_EQ(identity, ConnectionPair::clientIdentity); },  // onRemoteIdentity
        [](auto) { FAIL() << "Unexpected disconnect on server"; },                   // onRemoteDisconnect
        [](auto) { FAIL() << "Unexpected message on server"; },                      // onMessage

        // Client callbacks
        [](auto identity) { ASSERT_EQ(identity, ConnectionPair::serverIdentity); },  // onRemoteIdentity
        [](auto) { FAIL() << "Unexpected disconnect on client"; },                   // onRemoteDisconnect
        [](auto) { FAIL() << "Unexpected message on client"; }                       // onMessage
    );

    scaler::ymq::internal::MessageConnection& server = connections.server();
    scaler::ymq::internal::MessageConnection& client = connections.client();
    scaler::wrapper::uv::Loop& loop                  = connections.loop();

    ASSERT_FALSE(server.remoteIdentity().has_value());
    ASSERT_FALSE(client.remoteIdentity().has_value());

    while (!server.established() || !client.established()) {
        loop.run(UV_RUN_ONCE);
    }

    ASSERT_EQ(server.remoteIdentity(), ConnectionPair::clientIdentity);
    ASSERT_EQ(client.remoteIdentity(), ConnectionPair::serverIdentity);
}

TEST_F(YMQMessageConnectionTest, MessageExchange)
{
    // Test that two MessageConnections can exchange messages

    const std::string clientMessagePayload = "Hello from client";
    const std::string serverMessagePayload = "Hello from server";

    bool serverMessageReceived = false;
    bool clientMessageReceived = false;

    ConnectionPair connections(
        // Server callbacks
        []([[maybe_unused]] auto identity) {},                      // onRemoteIdentity
        [](auto) { FAIL() << "Unexpected disconnect on server"; },  // onRemoteDisconnect
        [&](scaler::ymq::Bytes messagePayload) {                    // onMessage
            auto payload = messagePayload.as_string();
            ASSERT_TRUE(payload.has_value());
            ASSERT_EQ(payload.value(), clientMessagePayload);
            serverMessageReceived = true;
        },

        // Client callbacks
        []([[maybe_unused]] auto identity) {},                      // onRemoteIdentity
        [](auto) { FAIL() << "Unexpected disconnect on client"; },  // onRemoteDisconnect
        [&](scaler::ymq::Bytes messagePayload) {                    // onMessage
            auto payload = messagePayload.as_string();
            ASSERT_TRUE(payload.has_value());
            ASSERT_EQ(payload.value(), serverMessagePayload);
            clientMessageReceived = true;
        });

    scaler::ymq::internal::MessageConnection& server = connections.server();
    scaler::ymq::internal::MessageConnection& client = connections.client();
    scaler::wrapper::uv::Loop& loop                  = connections.loop();

    // Send a message before the identity exchange
    scaler::ymq::Bytes messagePayload = scaler::ymq::Bytes(serverMessagePayload);
    server.sendMessage(std::move(messagePayload), [](auto result) { ASSERT_TRUE(result.has_value()); });

    // Wait for identity exchange
    while (!server.established() || !client.established()) {
        loop.run(UV_RUN_ONCE);
    }

    // Send a message after the identity exchange
    messagePayload = scaler::ymq::Bytes(clientMessagePayload);
    client.sendMessage(std::move(messagePayload), [](auto result) { ASSERT_TRUE(result.has_value()); });

    // Wait for the messages
    while (!serverMessageReceived || !clientMessageReceived) {
        loop.run(UV_RUN_ONCE);
    }
}

TEST_F(YMQMessageConnectionTest, Disconnect)
{
    // Test graceful disconnect (remote explicitly closes connection)

    bool serverDisconnected = false;

    ConnectionPair connections(
        // Server callbacks
        []([[maybe_unused]] auto identity) {},  // onRemoteIdentity
        [&](auto reason) {                      // onRemoteDisconnect
            ASSERT_EQ(reason, scaler::ymq::internal::MessageConnection::DisconnectReason::Disconnected);
            serverDisconnected = true;
        },
        [](auto) { FAIL() << "Unexpected message on server"; },  // onMessage

        // Client callbacks
        []([[maybe_unused]] auto identity) {},                      // onRemoteIdentity
        [](auto) { FAIL() << "Unexpected disconnect on client"; },  // onRemoteDisconnect
        [](auto) { FAIL() << "Unexpected message on client"; }      // onMessage
    );

    scaler::ymq::internal::MessageConnection& server = connections.server();
    scaler::ymq::internal::MessageConnection& client = connections.client();
    scaler::wrapper::uv::Loop& loop                  = connections.loop();

    // Wait for identity exchange
    while (!server.established() || !client.established()) {
        loop.run(UV_RUN_ONCE);
    }

    // Disconnect client after identity exchange
    client.disconnect();

    // Wait for server to detect disconnect
    while (!serverDisconnected) {
        loop.run(UV_RUN_ONCE);
    }

    ASSERT_FALSE(server.connected());
}

TEST_F(YMQMessageConnectionTest, UnexpectedDisconnect)
{
    // Test unexpected disconnect/abort (RST packet) using closeReset()

    ConnectionPair connections(
        // Server callbacks
        []([[maybe_unused]] auto identity) {},  // onRemoteIdentity
        [](auto reason) {                       // onRemoteDisconnect
            ASSERT_EQ(reason, scaler::ymq::internal::MessageConnection::DisconnectReason::Aborted);
        },
        [](auto) { FAIL() << "Unexpected message on server"; },  // onMessage

        // Client callbacks
        []([[maybe_unused]] auto identity) {},                      // onRemoteIdentity
        [](auto) { FAIL() << "Unexpected disconnect on client"; },  // onRemoteDisconnect
        [](auto) { FAIL() << "Unexpected message on client"; }      // onMessage
    );

    scaler::ymq::internal::MessageConnection& server = connections.server();
    scaler::ymq::internal::MessageConnection& client = connections.client();
    scaler::wrapper::uv::Loop& loop                  = connections.loop();

    // Wait for identity exchange
    while (!server.established() || !client.established()) {
        loop.run(UV_RUN_ONCE);
    }

    // Simulate unexpected disconnect (RST packet) on client socket
    client.abort();
    ASSERT_FALSE(client.connected());

    // Wait for server to detect disconnect
    while (server.connected()) {
        loop.run(UV_RUN_ONCE);
    }

    // Reconnect the client with a new TCP socket
    scaler::wrapper::uv::TCPSocket socket = UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPSocket::init(loop));
    UV_EXIT_ON_ERROR(socket.connect(connections.serverAddress(), [&](std::expected<void, scaler::wrapper::uv::Error>) {
        client.connect(std::move(socket));
    }));

    // Wait again for identity exchange
    while (!server.established() || !client.established()) {
        loop.run(UV_RUN_ONCE);
    }
}

TEST_F(YMQMessageConnectionTest, EmptyMessage)
{
    // Test that two MessageConnections can exchange empty messages

    bool serverMessageReceived = false;

    ConnectionPair connections(
        // Server callbacks
        []([[maybe_unused]] auto identity) {},                      // onRemoteIdentity
        [](auto) { FAIL() << "Unexpected disconnect on server"; },  // onRemoteDisconnect
        [&](scaler::ymq::Bytes messagePayload) {                    // onMessage
            ASSERT_EQ(messagePayload.as_string(), "");
            serverMessageReceived = true;
        },

        // Client callbacks
        []([[maybe_unused]] auto identity) {},                      // onRemoteIdentity
        [](auto) { FAIL() << "Unexpected disconnect on client"; },  // onRemoteDisconnect
        [](auto) { FAIL() << "Unexpected message on client"; }      // onMessage
    );

    scaler::ymq::internal::MessageConnection& server = connections.server();
    scaler::ymq::internal::MessageConnection& client = connections.client();
    scaler::wrapper::uv::Loop& loop                  = connections.loop();

    // Wait for identity exchange
    while (!server.established() || !client.established()) {
        loop.run(UV_RUN_ONCE);
    }

    // Send an empty message
    scaler::ymq::Bytes messagePayload = scaler::ymq::Bytes("");
    client.sendMessage(std::move(messagePayload), [](auto result) { ASSERT_TRUE(result.has_value()); });

    // Wait for the message
    while (!serverMessageReceived) {
        loop.run(UV_RUN_ONCE);
    }
}

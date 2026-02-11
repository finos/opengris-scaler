#include <gtest/gtest.h>

#include <expected>
#include <string>

#include "scaler/uv_ymq/address.h"
#include "scaler/uv_ymq/message_connection.h"
#include "scaler/wrapper/uv/callback.h"
#include "scaler/wrapper/uv/error.h"
#include "scaler/wrapper/uv/loop.h"
#include "scaler/wrapper/uv/tcp.h"
#include "scaler/ymq/bytes.h"

class UVYMQMessageConnectionTest: public ::testing::Test {};

// Helper class to set up a server and client message connection pair
class ConnectionPair {
public:
    static const scaler::uv_ymq::Identity serverIdentity;
    static const scaler::uv_ymq::Identity clientIdentity;

    ConnectionPair(
        scaler::wrapper::uv::Loop& loop,
        scaler::uv_ymq::MessageConnection::RemoteIdentityCallback serverOnIdentity,
        scaler::uv_ymq::MessageConnection::RemoteDisconnectCallback serverOnDisconnect,
        scaler::uv_ymq::MessageConnection::RecvMessageCallback serverOnMessage,
        scaler::uv_ymq::MessageConnection::RemoteIdentityCallback clientOnIdentity,
        scaler::uv_ymq::MessageConnection::RemoteDisconnectCallback clientOnDisconnect,
        scaler::uv_ymq::MessageConnection::RecvMessageCallback clientOnMessage)
        : _server(UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPServer::init(loop)))
        , _serverConnection(
              loop,
              serverIdentity,
              std::nullopt,
              std::move(serverOnIdentity),
              std::move(serverOnDisconnect),
              std::move(serverOnMessage))
        , _clientSocket(UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPSocket::init(loop)))
        , _clientConnection(
              loop,
              clientIdentity,
              std::nullopt,
              std::move(clientOnIdentity),
              std::move(clientOnDisconnect),
              std::move(clientOnMessage))
    {
        const auto listenAddress = scaler::uv_ymq::Address::fromString("tcp://127.0.0.1:0").value();
        UV_EXIT_ON_ERROR(_server.bind(listenAddress.asTCP(), uv_tcp_flags(0)));

        UV_EXIT_ON_ERROR(_server.listen(16, [&](std::expected<void, scaler::wrapper::uv::Error>) {
            scaler::wrapper::uv::TCPSocket serverSocket = UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPSocket::init(loop));
            UV_EXIT_ON_ERROR(_server.accept(serverSocket));

            _serverConnection.connect(std::move(serverSocket));
        }));

        UV_EXIT_ON_ERROR(
            _clientSocket.connect(serverAddress(), [this](std::expected<void, scaler::wrapper::uv::Error>) {
                _clientConnection.connect(std::move(_clientSocket));
            }));
    }

    scaler::wrapper::uv::SocketAddress serverAddress() const { return UV_EXIT_ON_ERROR(_server.getSockName()); }

    scaler::uv_ymq::MessageConnection& server() { return _serverConnection; }
    scaler::uv_ymq::MessageConnection& client() { return _clientConnection; }

private:
    scaler::wrapper::uv::TCPServer _server;
    scaler::uv_ymq::MessageConnection _serverConnection;

    scaler::wrapper::uv::TCPSocket _clientSocket;
    scaler::uv_ymq::MessageConnection _clientConnection;
};

const scaler::uv_ymq::Identity ConnectionPair::serverIdentity = "server-identity";
const scaler::uv_ymq::Identity ConnectionPair::clientIdentity = "client-identity";

TEST_F(UVYMQMessageConnectionTest, IdentityExchange)
{
    // Test that two MessageConnections successfully exchange identities

    scaler::wrapper::uv::Loop loop = UV_EXIT_ON_ERROR(scaler::wrapper::uv::Loop::init());

    ConnectionPair connections(
        loop,

        // Server callbacks
        [](auto identity) { ASSERT_EQ(identity, ConnectionPair::clientIdentity); },  // onRemoteIdentity
        [](auto) { FAIL() << "Unexpected disconnect on server"; },                   // onRemoteDisconnect
        [](auto) { FAIL() << "Unexpected message on server"; },                      // onMessage

        // Client callbacks
        [](auto identity) { ASSERT_EQ(identity, ConnectionPair::serverIdentity); },  // onRemoteIdentity
        [](auto) { FAIL() << "Unexpected disconnect on client"; },                   // onRemoteDisconnect
        [](auto) { FAIL() << "Unexpected message on client"; }                       // onMessage
    );

    scaler::uv_ymq::MessageConnection& server = connections.server();
    scaler::uv_ymq::MessageConnection& client = connections.client();

    ASSERT_FALSE(server.remoteIdentity().has_value());
    ASSERT_FALSE(client.remoteIdentity().has_value());

    while (!server.established() || !client.established()) {
        loop.run(UV_RUN_ONCE);
    }

    ASSERT_EQ(server.remoteIdentity(), ConnectionPair::clientIdentity);
    ASSERT_EQ(client.remoteIdentity(), ConnectionPair::serverIdentity);
}

TEST_F(UVYMQMessageConnectionTest, MessageExchange)
{
    // Test that two MessageConnections can exchange messages

    const std::string clientMessagePayload = "Hello from client";
    const std::string serverMessagePayload = "Hello from server";

    scaler::wrapper::uv::Loop loop = UV_EXIT_ON_ERROR(scaler::wrapper::uv::Loop::init());

    bool serverMessageReceived = false;
    bool clientMessageReceived = false;

    ConnectionPair connections(
        loop,

        // Server callbacks
        [](auto identity) {},                                       // onRemoteIdentity
        [](auto) { FAIL() << "Unexpected disconnect on server"; },  // onRemoteDisconnect
        [&](scaler::ymq::Bytes messagePayload) {                    // onMessage
            auto payload = messagePayload.as_string();
            ASSERT_TRUE(payload.has_value());
            ASSERT_EQ(payload.value(), clientMessagePayload);
            serverMessageReceived = true;
        },

        // Client callbacks
        [](auto identity) {},                                       // onRemoteIdentity
        [](auto) { FAIL() << "Unexpected disconnect on client"; },  // onRemoteDisconnect
        [&](scaler::ymq::Bytes messagePayload) {                    // onMessage
            auto payload = messagePayload.as_string();
            ASSERT_TRUE(payload.has_value());
            ASSERT_EQ(payload.value(), serverMessagePayload);
            clientMessageReceived = true;
        });

    scaler::uv_ymq::MessageConnection& server = connections.server();
    scaler::uv_ymq::MessageConnection& client = connections.client();

    // Send a message before the identity exchange
    scaler::ymq::Bytes messagePayload = scaler::ymq::Bytes(serverMessagePayload);
    connections.server().sendMessage(std::move(messagePayload), [](auto result) { ASSERT_TRUE(result.has_value()); });

    // Wait for identity exchange
    while (!server.established() || !client.established()) {
        loop.run(UV_RUN_ONCE);
    }

    // Send a message after the identity exchange
    messagePayload = scaler::ymq::Bytes(clientMessagePayload);
    connections.client().sendMessage(std::move(messagePayload), [](auto result) { ASSERT_TRUE(result.has_value()); });

    // Wait for the messages
    while (!serverMessageReceived || !clientMessageReceived) {
        loop.run(UV_RUN_ONCE);
    }
}

TEST_F(UVYMQMessageConnectionTest, Disconnect)
{
    // Test graceful disconnect (remote explicitly closes connection)

    scaler::wrapper::uv::Loop loop = UV_EXIT_ON_ERROR(scaler::wrapper::uv::Loop::init());

    bool serverDisconnected = false;

    ConnectionPair connections(
        loop,

        // Server callbacks
        [](auto identity) {},  // onRemoteIdentity
        [&](auto reason) {     // onRemoteDisconnect
            ASSERT_EQ(reason, scaler::uv_ymq::MessageConnection::DisconnectReason::Disconnected);
            serverDisconnected = true;
        },
        [](auto) { FAIL() << "Unexpected message on server"; },  // onMessage

        // Client callbacks
        [](auto identity) {},                                       // onRemoteIdentity
        [](auto) { FAIL() << "Unexpected disconnect on client"; },  // onRemoteDisconnect
        [](auto) { FAIL() << "Unexpected message on client"; }      // onMessage
    );

    scaler::uv_ymq::MessageConnection& server = connections.server();
    scaler::uv_ymq::MessageConnection& client = connections.client();

    // Wait for identity exchange
    while (!server.established() || !client.established()) {
        loop.run(UV_RUN_ONCE);
    }

    // Disconnect client after identity exchange
    connections.client().disconnect();

    // Wait for server to detect disconnect
    while (!serverDisconnected) {
        loop.run(UV_RUN_ONCE);
    }

    ASSERT_FALSE(connections.server().connected());
}

TEST_F(UVYMQMessageConnectionTest, UnexpectedDisconnect)
{
    // Test unexpected disconnect/abort (RST packet) using closeReset()

    scaler::wrapper::uv::Loop loop = UV_EXIT_ON_ERROR(scaler::wrapper::uv::Loop::init());

    ConnectionPair connections(
        loop,

        // Server callbacks
        [](auto identity) {},  // onRemoteIdentity
        [](auto reason) {      // onRemoteDisconnect
            ASSERT_EQ(reason, scaler::uv_ymq::MessageConnection::DisconnectReason::Aborted);
        },
        [](auto) { FAIL() << "Unexpected message on server"; },  // onMessage

        // Client callbacks
        [](auto identity) {},                                       // onRemoteIdentity
        [](auto) { FAIL() << "Unexpected disconnect on client"; },  // onRemoteDisconnect
        [](auto) { FAIL() << "Unexpected message on client"; }      // onMessage
    );

    scaler::uv_ymq::MessageConnection& server = connections.server();
    scaler::uv_ymq::MessageConnection& client = connections.client();

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

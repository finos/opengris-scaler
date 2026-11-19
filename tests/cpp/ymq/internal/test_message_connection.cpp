#include <gtest/gtest.h>

#ifdef __linux__
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#endif  // __linux__

#include <chrono>
#include <expected>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <thread>

#include "scaler/wrapper/uv/callback.h"
#include "scaler/wrapper/uv/error.h"
#include "scaler/wrapper/uv/loop.h"
#include "scaler/wrapper/uv/tcp.h"
#include "scaler/ymq/address.h"
#include "scaler/ymq/buffered_bytes.h"
#include "scaler/ymq/bytes.h"
#include "scaler/ymq/internal/accept_server.h"
#include "scaler/ymq/internal/connect_client.h"
#include "scaler/ymq/internal/message_connection.h"
#include "tests/cpp/ymq/common/utils.h"

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
              _loop,
              serverIdentity,
              std::nullopt,
              std::move(serverOnIdentity),
              std::move(serverOnDisconnect),
              std::move(serverOnMessage))
        , _clientSocket(UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPSocket::init(_loop)))
        , _clientConnection(
              _loop,
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
            ASSERT_EQ(uv_fileno(reinterpret_cast<const uv_handle_t*>(&serverSocket.handle().native()), &_serverFd), 0);

            _serverConnection.connect(scaler::ymq::internal::Client(std::move(serverSocket)));
        }));

        UV_EXIT_ON_ERROR(
            _clientSocket.connect(serverAddress(), [this](std::expected<void, scaler::wrapper::uv::Error>) {
                _clientConnection.connect(scaler::ymq::internal::Client(std::move(_clientSocket)));
            }));
    }

    scaler::wrapper::uv::SocketAddress serverAddress() const
    {
        return UV_EXIT_ON_ERROR(_server.getSockName());
    }

    scaler::wrapper::uv::Loop& loop()
    {
        return _loop;
    }
    scaler::ymq::internal::MessageConnection& server()
    {
        return _serverConnection;
    }
    scaler::ymq::internal::MessageConnection& client()
    {
        return _clientConnection;
    }
    uv_os_fd_t serverFd() const
    {
        return _serverFd;
    }

private:
    scaler::wrapper::uv::Loop _loop;
    scaler::wrapper::uv::TCPServer _server;
    scaler::ymq::internal::MessageConnection _serverConnection;
    uv_os_fd_t _serverFd {};

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
        [&](std::unique_ptr<scaler::ymq::Bytes> messagePayload) {   // onMessage
            auto payload = messagePayload->asString();
            ASSERT_TRUE(payload.has_value());
            ASSERT_EQ(payload.value(), clientMessagePayload);
            serverMessageReceived = true;
        },

        // Client callbacks
        []([[maybe_unused]] auto identity) {},                      // onRemoteIdentity
        [](auto) { FAIL() << "Unexpected disconnect on client"; },  // onRemoteDisconnect
        [&](std::unique_ptr<scaler::ymq::Bytes> messagePayload) {   // onMessage
            auto payload = messagePayload->asString();
            ASSERT_TRUE(payload.has_value());
            ASSERT_EQ(payload.value(), serverMessagePayload);
            clientMessageReceived = true;
        });

    scaler::ymq::internal::MessageConnection& server = connections.server();
    scaler::ymq::internal::MessageConnection& client = connections.client();
    scaler::wrapper::uv::Loop& loop                  = connections.loop();

    // Send a message before the identity exchange
    server.sendMessage(std::make_unique<scaler::ymq::BufferedBytes>(serverMessagePayload), [](auto result, auto) {
        ASSERT_TRUE(result.has_value());
    });

    // Wait for identity exchange
    while (!server.established() || !client.established()) {
        loop.run(UV_RUN_ONCE);
    }

    // Send a message after the identity exchange
    client.sendMessage(std::make_unique<scaler::ymq::BufferedBytes>(clientMessagePayload), [](auto result, auto) {
        ASSERT_TRUE(result.has_value());
    });

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
        client.connect(scaler::ymq::internal::Client(std::move(socket)));
    }));

    // Wait again for identity exchange
    while (!server.established() || !client.established()) {
        loop.run(UV_RUN_ONCE);
    }
}

#ifdef __linux__
TEST_F(YMQMessageConnectionTest, TimeoutSeenByAWriteIsStillAnAbort)
{
    // Like a reset, a timeout that a write consumed leaves a plain EOF for the next read, and must read as an abort.

    constexpr int userTimeoutMilliseconds = 500;
    constexpr int sendBufferBytes         = 8 * 1024 * 1024;
    constexpr size_t windowFillingBytes   = 1024 * 1024;  // more than the client's receive buffer holds
    constexpr std::chrono::seconds timeoutDeadline {10};
    constexpr std::chrono::milliseconds timeoutPollInterval {50};

    std::optional<scaler::ymq::internal::MessageConnection::DisconnectReason> serverDisconnectReason;

    ConnectionPair connections(
        // Server callbacks
        []([[maybe_unused]] auto identity) {},                  // onRemoteIdentity
        [&](auto reason) { serverDisconnectReason = reason; },  // onRemoteDisconnect
        []([[maybe_unused]] auto message) {},                   // onMessage

        // Client callbacks
        []([[maybe_unused]] auto identity) {},  // onRemoteIdentity
        []([[maybe_unused]] auto reason) {},    // onRemoteDisconnect
        []([[maybe_unused]] auto message) {}    // onMessage
    );

    scaler::ymq::internal::MessageConnection& server = connections.server();
    scaler::ymq::internal::MessageConnection& client = connections.client();
    scaler::wrapper::uv::Loop& loop                  = connections.loop();

    // Wait for identity exchange
    while (!server.established() || !client.established()) {
        loop.run(UV_RUN_ONCE);
    }

    // The loop does not run until the timeout, so the client stops reading and the kernel times out its zero window.
    const int serverFd = connections.serverFd();
    ASSERT_EQ(
        setsockopt(serverFd, IPPROTO_TCP, TCP_USER_TIMEOUT, &userTimeoutMilliseconds, sizeof(userTimeoutMilliseconds)),
        0);
    ASSERT_EQ(setsockopt(serverFd, SOL_SOCKET, SO_SNDBUF, &sendBufferBytes, sizeof(sendBufferBytes)), 0);

    server.sendMessage(
        std::make_unique<scaler::ymq::BufferedBytes>(std::string(windowFillingBytes, 'x')),
        []([[maybe_unused]] auto result, [[maybe_unused]] auto payload) {});

    auto tcpState = [&]() {
        tcp_info info {};
        socklen_t infoSize = sizeof(info);
        getsockopt(serverFd, IPPROTO_TCP, TCP_INFO, &info, &infoSize);
        return info.tcpi_state;
    };

    const auto deadline = std::chrono::steady_clock::now() + timeoutDeadline;
    while (tcpState() != TCP_CLOSE && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(timeoutPollInterval);
    }
    ASSERT_EQ(tcpState(), TCP_CLOSE) << "the kernel did not time the connection out";

    server.sendMessage(
        std::make_unique<scaler::ymq::BufferedBytes>("seen by the timeout"),
        []([[maybe_unused]] auto result, [[maybe_unused]] auto payload) {});

    while (!serverDisconnectReason.has_value()) {
        loop.run(UV_RUN_ONCE);
    }

    ASSERT_EQ(*serverDisconnectReason, scaler::ymq::internal::MessageConnection::DisconnectReason::Aborted);
}
#endif  // __linux__

TEST_F(YMQMessageConnectionTest, EmptyMessage)
{
    // Test that two MessageConnections can exchange empty messages

    bool serverMessageReceived = false;

    ConnectionPair connections(
        // Server callbacks
        []([[maybe_unused]] auto identity) {},                      // onRemoteIdentity
        [](auto) { FAIL() << "Unexpected disconnect on server"; },  // onRemoteDisconnect
        [&](std::unique_ptr<scaler::ymq::Bytes> messagePayload) {   // onMessage
            ASSERT_EQ(messagePayload->asString(), "");
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
    client.sendMessage(
        std::make_unique<scaler::ymq::BufferedBytes>(""), [](auto result, auto) { ASSERT_TRUE(result.has_value()); });

    // Wait for the message
    while (!serverMessageReceived) {
        loop.run(UV_RUN_ONCE);
    }
}

TEST_F(YMQMessageConnectionTest, InvalidMagicString)
{
    // Check the message connection abort a connection if it does not receive the expected magic string

    scaler::wrapper::uv::Loop loop = UV_EXIT_ON_ERROR(scaler::wrapper::uv::Loop::init());

    // Create a TCP server that always answer with a HTTP response

    const std::string_view httpResponse = "HTTP/1.1 200 OK\n";

    scaler::wrapper::uv::TCPServer server = UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPServer::init(loop));

    const auto listenAddress = scaler::ymq::Address::fromString("tcp://127.0.0.1:0").value();
    UV_EXIT_ON_ERROR(server.bind(listenAddress.asTCP(), uv_tcp_flags(0)));

    UV_EXIT_ON_ERROR(server.listen(16, [&](std::expected<void, scaler::wrapper::uv::Error>) {
        auto serverSocket = std::make_unique<scaler::wrapper::uv::TCPSocket>(
            UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPSocket::init(loop)));
        UV_EXIT_ON_ERROR(server.accept(*serverSocket));

        const std::span<const uint8_t> payload {
            reinterpret_cast<const uint8_t*>(httpResponse.data()), httpResponse.size()};

        UV_EXIT_ON_ERROR(serverSocket->write(
            std::span(&payload, 1),
            [serverSocket = std::move(serverSocket)](auto result) mutable { UV_EXIT_ON_ERROR(result); }));
    }));

    // Make the message connection connect to this HTTP server

    bool aborted = false;

    scaler::wrapper::uv::TCPSocket clientSocket = UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPSocket::init(loop));
    scaler::ymq::internal::MessageConnection clientConnection(
        loop,
        "client-identity",
        std::nullopt,
        [](auto) { FAIL() << "Unexpected identity callback"; },
        [&](auto reason) {
            ASSERT_EQ(reason, scaler::ymq::internal::MessageConnection::DisconnectReason::Aborted);
            aborted = true;
        },
        [](auto) { FAIL() << "Unexpected message callback"; });

    UV_EXIT_ON_ERROR(clientSocket.connect(
        UV_EXIT_ON_ERROR(server.getSockName()), [&](std::expected<void, scaler::wrapper::uv::Error>) {
            clientConnection.connect(scaler::ymq::internal::Client(std::move(clientSocket)));
        }));

    // Wait until the message connection raise an abort event

    while (!aborted) {
        loop.run(UV_RUN_ONCE);
    }

    ASSERT_FALSE(clientConnection.connected());
}

TEST_F(YMQMessageConnectionTest, ConnectionLostUnderInFlightSend)
{
    // An abort fails a send in flight with ConnectionLost, not SocketStopRequested: nobody stopped the connection.
    // An invalid magic string makes the connection abort itself, so the write is canceled, never failed by the OS.

    constexpr size_t stalledSendBytes = 64 * 1024 * 1024;  // more than loopback's socket buffers hold

    scaler::wrapper::uv::Loop loop = UV_EXIT_ON_ERROR(scaler::wrapper::uv::Loop::init());

    // Create a TCP server that answers with a HTTP response and never reads

    const std::string_view httpResponse = "HTTP/1.1 200 OK\n";

    scaler::wrapper::uv::TCPServer server = UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPServer::init(loop));

    const auto listenAddress = scaler::ymq::Address::fromString("tcp://127.0.0.1:0").value();
    UV_EXIT_ON_ERROR(server.bind(listenAddress.asTCP(), uv_tcp_flags(0)));

    std::optional<scaler::wrapper::uv::TCPSocket> serverSocket;

    UV_EXIT_ON_ERROR(server.listen(16, [&](std::expected<void, scaler::wrapper::uv::Error>) {
        serverSocket = UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPSocket::init(loop));
        UV_EXIT_ON_ERROR(server.accept(*serverSocket));

        const std::span<const uint8_t> payload {
            reinterpret_cast<const uint8_t*>(httpResponse.data()), httpResponse.size()};

        UV_EXIT_ON_ERROR(serverSocket->write(std::span(&payload, 1), [](auto result) { UV_EXIT_ON_ERROR(result); }));
    }));

    // Send a message that the connection writes once connected, and that stays in flight as the server never reads

    std::optional<std::expected<void, scaler::ymq::Error>> sendResult;

    scaler::wrapper::uv::TCPSocket clientSocket = UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPSocket::init(loop));
    scaler::ymq::internal::MessageConnection clientConnection(
        loop,
        "client-identity",
        std::nullopt,
        [](auto) { FAIL() << "Unexpected identity callback"; },
        []([[maybe_unused]] auto reason) {},
        [](auto) { FAIL() << "Unexpected message callback"; });

    clientConnection.sendMessage(
        std::make_unique<scaler::ymq::BufferedBytes>(std::string(stalledSendBytes, 'x')),
        [&](auto result, [[maybe_unused]] auto payload) { sendResult = std::move(result); });

    UV_EXIT_ON_ERROR(clientSocket.connect(
        UV_EXIT_ON_ERROR(server.getSockName()), [&](std::expected<void, scaler::wrapper::uv::Error>) {
            clientConnection.connect(scaler::ymq::internal::Client(std::move(clientSocket)));
        }));

    while (!sendResult.has_value()) {
        loop.run(UV_RUN_ONCE);
    }

    ASSERT_FALSE(clientConnection.connected());
    ASSERT_FALSE(sendResult->has_value());
    ASSERT_EQ(sendResult->error()._errorCode, scaler::ymq::Error::ErrorCode::ConnectionLost);
}

// A server and a client message connection over the given transport, both on one loop.
class TransportConnectionPair {
public:
    TransportConnectionPair(
        const std::string& transport,
        scaler::ymq::internal::MessageConnection::RemoteDisconnectCallback serverOnDisconnect,
        scaler::ymq::internal::MessageConnection::RecvMessageCallback serverOnMessage)
        : _loop(UV_EXIT_ON_ERROR(scaler::wrapper::uv::Loop::init()))
        , _serverConnection(
              _loop,
              ConnectionPair::serverIdentity,
              std::nullopt,
              []([[maybe_unused]] auto identity) {},
              std::move(serverOnDisconnect),
              std::move(serverOnMessage))
        , _clientConnection(
              _loop,
              ConnectionPair::clientIdentity,
              std::nullopt,
              []([[maybe_unused]] auto identity) {},
              []([[maybe_unused]] auto reason) {},
              []([[maybe_unused]] auto message) {})
    {
        const auto listenAddress =
            scaler::ymq::Address::fromString(getTransportAddress(transport, 0), getTLSConfig(transport)).value();

        _server = scaler::ymq::internal::AcceptServer::init(
                      _loop,
                      listenAddress,
                      [this](scaler::ymq::internal::Client client) { _serverConnection.connect(std::move(client)); })
                      .value();

        const auto serverAddress = scaler::ymq::Address::fromString(_server->address().toString().value()).value();

        _connectClient = scaler::ymq::internal::ConnectClient::init(
                             _loop,
                             serverAddress,
                             [this](std::expected<scaler::ymq::internal::Client, scaler::ymq::Error> client) {
                                 _clientConnection.connect(std::move(client.value()));
                             })
                             .value();
    }

    scaler::wrapper::uv::Loop& loop()
    {
        return _loop;
    }
    scaler::ymq::internal::MessageConnection& server()
    {
        return _serverConnection;
    }
    scaler::ymq::internal::MessageConnection& client()
    {
        return _clientConnection;
    }

private:
    scaler::wrapper::uv::Loop _loop;
    scaler::ymq::internal::MessageConnection _serverConnection;
    scaler::ymq::internal::MessageConnection _clientConnection;
    std::optional<scaler::ymq::internal::AcceptServer> _server;
    std::optional<scaler::ymq::internal::ConnectClient> _connectClient;
};

class YMQMessageConnectionTransportTest: public ::testing::TestWithParam<std::string> {};

TEST_P(YMQMessageConnectionTransportTest, ResetSeenByAWriteIsStillAnAbort)
{
    // A write that sees a reset consumes the socket's error, so the next read returns a plain EOF. The remote did not
    // ask to disconnect, so this must read as an abort: a connector never reconnects after a graceful disconnect.

    std::optional<scaler::ymq::internal::MessageConnection::DisconnectReason> serverDisconnectReason;

    TransportConnectionPair connections(
        GetParam(),
        [&](auto reason) { serverDisconnectReason = reason; },  // server onRemoteDisconnect
        []([[maybe_unused]] auto message) {}                    // server onMessage
    );

    scaler::ymq::internal::MessageConnection& server = connections.server();
    scaler::ymq::internal::MessageConnection& client = connections.client();
    scaler::wrapper::uv::Loop& loop                  = connections.loop();

    // Wait for identity exchange
    while (!server.established() || !client.established()) {
        loop.run(UV_RUN_ONCE);
    }

    client.abort();

    // Both ends share this thread: the reset has reached the server's socket, and its loop has not run since.
    server.sendMessage(
        std::make_unique<scaler::ymq::BufferedBytes>("seen by the reset"),
        []([[maybe_unused]] auto result, [[maybe_unused]] auto payload) {});

    while (!serverDisconnectReason.has_value()) {
        loop.run(UV_RUN_ONCE);
    }

    ASSERT_EQ(*serverDisconnectReason, scaler::ymq::internal::MessageConnection::DisconnectReason::Aborted);
}

TEST_P(YMQMessageConnectionTransportTest, ResetSeenByAReplyIsStillAnAbort)
{
    // A reply from the read callback consumes a reset that arrived with the request, and libuv reports the EOF before
    // the reply's write callback. BinderSocket replies this way when it flushes the messages queued for a peer.

    std::optional<scaler::ymq::internal::MessageConnection::DisconnectReason> serverDisconnectReason;
    scaler::ymq::internal::MessageConnection* replier = nullptr;

    TransportConnectionPair connections(
        GetParam(),
        [&](auto reason) { serverDisconnectReason = reason; },  // server onRemoteDisconnect
        [&]([[maybe_unused]] auto message) {                    // server onMessage
            replier->sendMessage(
                std::make_unique<scaler::ymq::BufferedBytes>("reply"),
                []([[maybe_unused]] auto result, [[maybe_unused]] auto payload) {});
        });

    scaler::ymq::internal::MessageConnection& server = connections.server();
    scaler::ymq::internal::MessageConnection& client = connections.client();
    scaler::wrapper::uv::Loop& loop                  = connections.loop();
    replier                                          = &server;

    // Wait for identity exchange
    while (!server.established() || !client.established()) {
        loop.run(UV_RUN_ONCE);
    }

    // The request and the reset reach the server's socket together.
    client.sendMessage(
        std::make_unique<scaler::ymq::BufferedBytes>("request"),
        []([[maybe_unused]] auto result, [[maybe_unused]] auto payload) {});
    client.abort();

    while (!serverDisconnectReason.has_value()) {
        loop.run(UV_RUN_ONCE);
    }

    ASSERT_EQ(*serverDisconnectReason, scaler::ymq::internal::MessageConnection::DisconnectReason::Aborted);
}

// IPC has no reset: an abort closes the pipe like a graceful disconnect.
INSTANTIATE_TEST_SUITE_P(
    YMQTransport,
    YMQMessageConnectionTransportTest,
    ::testing::Values("tcp", "tls", "ws", "wss"),
    [](const testing::TestParamInfo<YMQMessageConnectionTransportTest::ParamType>& info) { return info.param; });

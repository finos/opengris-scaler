#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "scaler/wrapper/uv/error.h"
#include "scaler/wrapper/uv/loop.h"
#include "scaler/wrapper/uv/socket_address.h"
#include "scaler/wrapper/uv/tcp.h"
#include "scaler/ymq/address.h"
#include "scaler/ymq/internal/accept_server.h"
#include "scaler/ymq/internal/connect_client.h"
#include "scaler/ymq/internal/websocket_stream.h"

class WebSocketStreamTest: public ::testing::Test {};

// ─── Test helpers ─────────────────────────────────────────────────────────────

namespace {

// Creates a pair of connected TCP sockets via a local loopback server.
// Not movable — lambdas capture 'this' by pointer.
struct TestTCPPair {
    scaler::wrapper::uv::Loop& _loop;
    std::optional<scaler::wrapper::uv::TCPServer> _server;
    std::optional<scaler::wrapper::uv::TCPSocket> _client;
    std::optional<scaler::wrapper::uv::TCPSocket> _serverSide;
    bool _clientConnected = false;

    explicit TestTCPPair(scaler::wrapper::uv::Loop& loop): _loop(loop)
    {
        _server.emplace(UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPServer::init(_loop)));
        UV_EXIT_ON_ERROR(
            _server->bind(UV_EXIT_ON_ERROR(scaler::wrapper::uv::SocketAddress::IPv4("127.0.0.1", 0)), uv_tcp_flags {}));
        UV_EXIT_ON_ERROR(_server->listen(1, [this](std::expected<void, scaler::wrapper::uv::Error> result) {
            UV_EXIT_ON_ERROR(result);
            auto sock = UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPSocket::init(_loop));
            UV_EXIT_ON_ERROR(_server->accept(sock));
            _serverSide.emplace(std::move(sock));
        }));

        const int port = UV_EXIT_ON_ERROR(_server->getSockName()).port();
        _client.emplace(UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPSocket::init(_loop)));
        UV_EXIT_ON_ERROR(_client->connect(
            UV_EXIT_ON_ERROR(scaler::wrapper::uv::SocketAddress::IPv4("127.0.0.1", port)),
            [this](std::expected<void, scaler::wrapper::uv::Error> r) {
                UV_EXIT_ON_ERROR(r);
                _clientConnected = true;
            }));
    }

    TestTCPPair(const TestTCPPair&)            = delete;
    TestTCPPair& operator=(const TestTCPPair&) = delete;
    TestTCPPair(TestTCPPair&&)                 = delete;
    TestTCPPair& operator=(TestTCPPair&&)      = delete;

    // Wait for both sides to be ready before attempting any reads/writes.
    void waitForFullConnection()
    {
        while (!_serverSide.has_value() || !_clientConnected)
            _loop.run(UV_RUN_ONCE);
    }
};

// Builds a masked WebSocket frame (client→server) for payloads up to 125 bytes.
// byte0 encodes FIN and opcode: 0x82 (FIN|binary), 0x02 (FIN=0|binary),
// 0x00 (FIN=0|continuation), 0x80 (FIN=1|continuation), 0x89 (FIN|ping).
std::vector<uint8_t> maskedFrame(uint8_t byte0, std::vector<uint8_t> payload)
{
    const std::array<uint8_t, 4> maskKey = {0xDE, 0xAD, 0xBE, 0xEF};
    std::vector<uint8_t> frame;
    frame.push_back(byte0);
    frame.push_back(0x80 | static_cast<uint8_t>(payload.size()));
    frame.insert(frame.end(), maskKey.begin(), maskKey.end());
    for (size_t i = 0; i < payload.size(); ++i)
        frame.push_back(payload[i] ^ maskKey[i % 4]);
    return frame;
}

void runUntil(scaler::wrapper::uv::Loop& loop, const bool& done)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (!done && std::chrono::steady_clock::now() < deadline)
        loop.run(UV_RUN_ONCE);
}

}  // namespace

// ─── Address parsing tests ────────────────────────────────────────────────────

// Verify that a WebSocket address round-trips through toString() / fromString().
TEST_F(WebSocketStreamTest, AddressRoundTrip)
{
    const auto addr = scaler::ymq::Address::fromString("ws://127.0.0.1:8765/");
    ASSERT_TRUE(addr.has_value());
    ASSERT_EQ(addr->type(), scaler::ymq::Address::Type::WebSocket);

    const auto& ws = addr->asWebSocket();
    EXPECT_EQ(ws.host, "127.0.0.1");
    EXPECT_EQ(ws.port, 8765);
    EXPECT_EQ(ws.path, "/");
    EXPECT_FALSE(ws.secure);

    const auto str = addr->toString();
    ASSERT_TRUE(str.has_value());
    EXPECT_EQ(*str, "ws://127.0.0.1:8765/");
}

TEST_F(WebSocketStreamTest, AddressWithPath)
{
    const auto addr = scaler::ymq::Address::fromString("ws://127.0.0.1:9000/ymq/v1");
    ASSERT_TRUE(addr.has_value());
    ASSERT_EQ(addr->asWebSocket().path, "/ymq/v1");
    ASSERT_FALSE(addr->asWebSocket().secure);
}

TEST_F(WebSocketStreamTest, WSSAddress)
{
    const auto addr = scaler::ymq::Address::fromString("wss://127.0.0.1:443/");
    ASSERT_TRUE(addr.has_value());
    ASSERT_EQ(addr->type(), scaler::ymq::Address::Type::WebSocket);
    ASSERT_TRUE(addr->asWebSocket().secure);

    const auto str = addr->toString();
    ASSERT_TRUE(str.has_value());
    ASSERT_TRUE(str->starts_with("wss://"));
}

TEST_F(WebSocketStreamTest, InvalidAddresses)
{
    EXPECT_FALSE(scaler::ymq::Address::fromString("ws://127.0.0.1").has_value());       // missing port
    EXPECT_FALSE(scaler::ymq::Address::fromString("ws://127.0.0.1:abc/").has_value());  // bad port
}

// ─── End-to-end handshake test ────────────────────────────────────────────────

// End-to-end: a WebSocket AcceptServer receives a connection.
// Verifies that the full upgrade path runs without error and that the server's
// connection callback is invoked with an isWebSocket() client.
TEST_F(WebSocketStreamTest, ClientServerHandshake)
{
    scaler::wrapper::uv::Loop loop = UV_EXIT_ON_ERROR(scaler::wrapper::uv::Loop::init());

    const auto listenAddress = scaler::ymq::Address::fromString("ws://127.0.0.1:0/").value();

    std::vector<uint8_t> serverReceived {};
    // Keep the server-side client alive across the callback boundary.
    std::optional<scaler::ymq::internal::Client> serverClient {};

    // ── Server ──────────────────────────────────────────────────────────────
    scaler::ymq::internal::AcceptServer server(loop, listenAddress, [&](scaler::ymq::internal::Client client) {
        ASSERT_TRUE(client.isWebSocket());
        serverClient.emplace(std::move(client));
        UV_EXIT_ON_ERROR(serverClient->readStart(
            [&serverReceived](std::expected<std::span<const uint8_t>, scaler::wrapper::uv::Error> result) {
                if (!result.has_value())
                    return;
                serverReceived.insert(serverReceived.end(), result->begin(), result->end());
            }));
    });

    const scaler::ymq::Address boundAddress = server.address();
    ASSERT_EQ(boundAddress.type(), scaler::ymq::Address::Type::WebSocket);

    // ── Client ──────────────────────────────────────────────────────────────
    bool clientConnected = false;
    // Keep the client-side client alive so the socket stays open for writing.
    std::optional<scaler::ymq::internal::Client> clientClient {};
    const std::vector<uint8_t> msg {'H', 'e', 'l', 'l', 'o'};

    scaler::ymq::internal::ConnectClient connector(
        loop, boundAddress, [&](std::expected<scaler::ymq::internal::Client, scaler::ymq::Error> result) {
            ASSERT_TRUE(result.has_value());
            ASSERT_TRUE(result->isWebSocket());
            clientConnected = true;

            clientClient.emplace(std::move(*result));
            const std::span<const uint8_t> msgSpan(msg.data(), msg.size());
            UV_EXIT_ON_ERROR(clientClient->write(std::span<const std::span<const uint8_t>>(&msgSpan, 1), [](auto) {}));
        });

    // Run the event loop until the server receives the message (or timeout after 2 s).
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (serverReceived.empty() && std::chrono::steady_clock::now() < deadline) {
        loop.run(UV_RUN_ONCE);
    }

    EXPECT_TRUE(clientConnected);
    EXPECT_EQ(serverReceived, (std::vector<uint8_t> {'H', 'e', 'l', 'l', 'o'}));

    // Clean up before the loop is destroyed: readStop breaks the shared_ptr cycle inside
    // WebSocketStream::readStart, then closeReset closes the TCP connection.
    if (serverClient) {
        serverClient->readStop();
        UV_EXIT_ON_ERROR(serverClient->closeReset());
        serverClient.reset();
    }
    if (clientClient) {
        clientClient->readStop();
        UV_EXIT_ON_ERROR(clientClient->closeReset());
        clientClient.reset();
    }
    server.disconnect();
    connector.disconnect();
    loop.run(UV_RUN_DEFAULT);
}

// ─── Protocol-level tests using raw TCP + fromUpgradedSocket ─────────────────

// Verifies that three frames (FIN=0 binary, FIN=0 continuation, FIN=1 continuation)
// are assembled into a single message delivery.
TEST_F(WebSocketStreamTest, FragmentedMessage)
{
    scaler::wrapper::uv::Loop loop = UV_EXIT_ON_ERROR(scaler::wrapper::uv::Loop::init());
    TestTCPPair pair(loop);
    pair.waitForFullConnection();

    // Wrap the server side in a WebSocketStream (bypass the HTTP upgrade).
    auto wsStream = scaler::ymq::internal::WebSocketStream::fromUpgradedSocket(
        std::move(pair._serverSide.value()), true /* server */);
    pair._serverSide.reset();
    pair._server.reset();

    std::vector<uint8_t> received;
    bool done = false;

    UV_EXIT_ON_ERROR(
        wsStream.readStart([&](std::expected<std::span<const uint8_t>, scaler::wrapper::uv::Error> result) {
            if (!result.has_value())
                return;
            received.insert(received.end(), result->begin(), result->end());
            done = true;
        }));

    // Build and send three fragmented frames from the client:
    //   frame1: FIN=0, opcode=binary (0x02), payload "Hello"
    //   frame2: FIN=0, opcode=continuation (0x00), payload " "
    //   frame3: FIN=1, opcode=continuation (0x80), payload "World"
    auto frame1 = maskedFrame(0x02, {'H', 'e', 'l', 'l', 'o'});
    auto frame2 = maskedFrame(0x00, {' '});
    auto frame3 = maskedFrame(0x80, {'W', 'o', 'r', 'l', 'd'});

    auto allFrames = std::make_shared<std::vector<uint8_t>>();
    allFrames->insert(allFrames->end(), frame1.begin(), frame1.end());
    allFrames->insert(allFrames->end(), frame2.begin(), frame2.end());
    allFrames->insert(allFrames->end(), frame3.begin(), frame3.end());

    const std::span<const uint8_t> frameSpan(*allFrames);
    UV_EXIT_ON_ERROR(pair._client->write(
        std::span<const std::span<const uint8_t>>(&frameSpan, 1),
        [allFrames](std::expected<void, scaler::wrapper::uv::Error>) {}));

    runUntil(loop, done);

    EXPECT_TRUE(done);
    EXPECT_EQ(received, (std::vector<uint8_t> {'H', 'e', 'l', 'l', 'o', ' ', 'W', 'o', 'r', 'l', 'd'}));

    wsStream.readStop();
    UV_EXIT_ON_ERROR(wsStream.closeReset());
    UV_EXIT_ON_ERROR(pair._client->closeReset());
    loop.run(UV_RUN_DEFAULT);
}

// Verifies that a PING frame from the client causes the server to send back a PONG.
TEST_F(WebSocketStreamTest, PingReceivesPong)
{
    scaler::wrapper::uv::Loop loop = UV_EXIT_ON_ERROR(scaler::wrapper::uv::Loop::init());
    TestTCPPair pair(loop);
    pair.waitForFullConnection();

    auto wsStream = scaler::ymq::internal::WebSocketStream::fromUpgradedSocket(
        std::move(pair._serverSide.value()), true /* server */);
    pair._serverSide.reset();
    pair._server.reset();

    UV_EXIT_ON_ERROR(wsStream.readStart([](std::expected<std::span<const uint8_t>, scaler::wrapper::uv::Error>) {}));

    // Read raw bytes from the client side to detect the PONG.
    std::vector<uint8_t> clientReceived;
    bool pongReceived = false;

    UV_EXIT_ON_ERROR(
        pair._client->readStart([&](std::expected<std::span<const uint8_t>, scaler::wrapper::uv::Error> result) {
            if (!result.has_value())
                return;
            clientReceived.insert(clientReceived.end(), result->begin(), result->end());
            // PONG frame from server (unmasked): 0x8A (FIN|pong) followed by length byte.
            if (clientReceived.size() >= 2 && clientReceived[0] == 0x8A)
                pongReceived = true;
        }));

    // Send a masked PING frame (opcode 0x9) with no payload.
    auto pingFrame = maskedFrame(0x89, {});
    auto pingData  = std::make_shared<std::vector<uint8_t>>(std::move(pingFrame));
    const std::span<const uint8_t> pingSpan(*pingData);
    UV_EXIT_ON_ERROR(pair._client->write(
        std::span<const std::span<const uint8_t>>(&pingSpan, 1),
        [pingData](std::expected<void, scaler::wrapper::uv::Error>) {}));

    runUntil(loop, pongReceived);

    EXPECT_TRUE(pongReceived);
    ASSERT_GE(clientReceived.size(), 2u);
    EXPECT_EQ(clientReceived[0], 0x8A);  // FIN=1, opcode=pong
    EXPECT_EQ(clientReceived[1], 0x00);  // unmasked, no payload

    wsStream.readStop();
    pair._client->readStop();
    UV_EXIT_ON_ERROR(wsStream.closeReset());
    UV_EXIT_ON_ERROR(pair._client->closeReset());
    loop.run(UV_RUN_DEFAULT);
}

// Verifies that a frame advertising a payload larger than kMaxWebSocketPayloadSize
// causes an error to be delivered to the read callback.
TEST_F(WebSocketStreamTest, OversizedPayloadError)
{
    scaler::wrapper::uv::Loop loop = UV_EXIT_ON_ERROR(scaler::wrapper::uv::Loop::init());
    TestTCPPair pair(loop);
    pair.waitForFullConnection();

    auto wsStream = scaler::ymq::internal::WebSocketStream::fromUpgradedSocket(
        std::move(pair._serverSide.value()), true /* server */);
    pair._serverSide.reset();
    pair._server.reset();

    bool errorReceived = false;

    UV_EXIT_ON_ERROR(
        wsStream.readStart([&](std::expected<std::span<const uint8_t>, scaler::wrapper::uv::Error> result) {
            if (!result.has_value())
                errorReceived = true;
        }));

    // Build a 10-byte frame header with payloadLen = 64 MiB + 1.
    // byte0 = FIN=1, binary; byte1 = MASK=1, extended-64 marker (0xFF = 0x80|127).
    // The 8-byte length field encodes 64*1024*1024 + 1 = 0x0000000004000001.
    const auto oversizedData = std::make_shared<std::vector<uint8_t>>(std::vector<uint8_t> {
        0x82,
        0xFF,
        0x00,
        0x00,
        0x00,
        0x00,
        0x04,
        0x00,
        0x00,
        0x01,
    });

    const std::span<const uint8_t> oversizedSpan(*oversizedData);
    UV_EXIT_ON_ERROR(pair._client->write(
        std::span<const std::span<const uint8_t>>(&oversizedSpan, 1),
        [oversizedData](std::expected<void, scaler::wrapper::uv::Error>) {}));

    runUntil(loop, errorReceived);

    EXPECT_TRUE(errorReceived);

    wsStream.readStop();
    UV_EXIT_ON_ERROR(wsStream.closeReset());
    UV_EXIT_ON_ERROR(pair._client->closeReset());
    loop.run(UV_RUN_DEFAULT);
}

// Verifies that the server terminates the upgrade if the client sends more than
// kMaxUpgradeHeaderSize bytes before the end-of-headers delimiter.
TEST_F(WebSocketStreamTest, UpgradeHeaderTooLarge)
{
    scaler::wrapper::uv::Loop loop = UV_EXIT_ON_ERROR(scaler::wrapper::uv::Loop::init());
    TestTCPPair pair(loop);
    pair.waitForFullConnection();

    bool upgradeErrorReceived = false;

    scaler::ymq::internal::WebSocketStream::upgradeAsServer(
        std::move(pair._serverSide.value()),
        [&](std::expected<scaler::ymq::internal::WebSocketStream, scaler::wrapper::uv::Error> result) {
            if (!result.has_value())
                upgradeErrorReceived = true;
        });
    pair._serverSide.reset();
    pair._server.reset();

    // Send 65 KiB of garbage with no "\r\n\r\n" to overflow the upgrade buffer.
    const auto garbage = std::make_shared<std::vector<uint8_t>>(65 * 1024, static_cast<uint8_t>('X'));
    const std::span<const uint8_t> garbageSpan(*garbage);
    UV_EXIT_ON_ERROR(pair._client->write(
        std::span<const std::span<const uint8_t>>(&garbageSpan, 1),
        [garbage](std::expected<void, scaler::wrapper::uv::Error>) {}));

    runUntil(loop, upgradeErrorReceived);

    EXPECT_TRUE(upgradeErrorReceived);

    UV_EXIT_ON_ERROR(pair._client->closeReset());
    loop.run(UV_RUN_DEFAULT);
}

// ─── HTTP upgrade validation tests ───────────────────────────────────────────

// Runs upgradeAsServer with a raw HTTP request and returns whether it succeeded.
static bool upgradeSucceeds(const std::string& request)
{
    scaler::wrapper::uv::Loop loop = UV_EXIT_ON_ERROR(scaler::wrapper::uv::Loop::init());
    TestTCPPair pair(loop);
    pair.waitForFullConnection();

    std::optional<bool> upgradeResult;
    std::optional<scaler::ymq::internal::WebSocketStream> upgradedStream;

    scaler::ymq::internal::WebSocketStream::upgradeAsServer(
        std::move(pair._serverSide.value()),
        [&](std::expected<scaler::ymq::internal::WebSocketStream, scaler::wrapper::uv::Error> result) {
            upgradeResult = result.has_value();
            if (result.has_value())
                upgradedStream.emplace(std::move(*result));
        });
    pair._serverSide.reset();
    pair._server.reset();

    auto requestData = std::make_shared<std::string>(request);
    const std::span<const uint8_t> requestSpan(
        reinterpret_cast<const uint8_t*>(requestData->data()), requestData->size());
    UV_EXIT_ON_ERROR(pair._client->write(
        std::span<const std::span<const uint8_t>>(&requestSpan, 1),
        [requestData](std::expected<void, scaler::wrapper::uv::Error>) {}));

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (!upgradeResult.has_value() && std::chrono::steady_clock::now() < deadline)
        loop.run(UV_RUN_ONCE);

    if (upgradedStream) {
        upgradedStream->readStop();
        UV_EXIT_ON_ERROR(upgradedStream->closeReset());
    }
    UV_EXIT_ON_ERROR(pair._client->closeReset());
    loop.run(UV_RUN_DEFAULT);

    return upgradeResult.value_or(false);
}

static const std::string kValidUpgrade =
    "GET / HTTP/1.1\r\n"
    "Host: 127.0.0.1\r\n"
    "Upgrade: websocket\r\n"
    "Connection: Upgrade\r\n"
    "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
    "Sec-WebSocket-Version: 13\r\n"
    "\r\n";

TEST_F(WebSocketStreamTest, UpgradeValidRequestSucceeds)
{
    EXPECT_TRUE(upgradeSucceeds(kValidUpgrade));
}

TEST_F(WebSocketStreamTest, UpgradeBadMethod)
{
    const std::string request =
        "POST / HTTP/1.1\r\n"
        "Host: 127.0.0.1\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
        "Sec-WebSocket-Version: 13\r\n"
        "\r\n";
    EXPECT_FALSE(upgradeSucceeds(request));
}

TEST_F(WebSocketStreamTest, UpgradeMissingConnectionHeader)
{
    const std::string request =
        "GET / HTTP/1.1\r\n"
        "Host: 127.0.0.1\r\n"
        "Upgrade: websocket\r\n"
        "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
        "Sec-WebSocket-Version: 13\r\n"
        "\r\n";
    EXPECT_FALSE(upgradeSucceeds(request));
}

TEST_F(WebSocketStreamTest, UpgradeWrongVersion)
{
    const std::string request =
        "GET / HTTP/1.1\r\n"
        "Host: 127.0.0.1\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
        "Sec-WebSocket-Version: 12\r\n"
        "\r\n";
    EXPECT_FALSE(upgradeSucceeds(request));
}

// Verifies that "Upgrade:websocket" (no space after colon) is accepted.
TEST_F(WebSocketStreamTest, UpgradeHeaderNoSpaceAfterColon)
{
    const std::string request =
        "GET / HTTP/1.1\r\n"
        "Host: 127.0.0.1\r\n"
        "Upgrade:websocket\r\n"
        "Connection:Upgrade\r\n"
        "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
        "Sec-WebSocket-Version:13\r\n"
        "\r\n";
    EXPECT_TRUE(upgradeSucceeds(request));
}

// ─── Graceful shutdown test ───────────────────────────────────────────────────

// Verifies that calling shutdown() on a WebSocketStream causes the peer to receive
// a WebSocket CLOSE frame (opcode 0x8) before the TCP FIN.
TEST_F(WebSocketStreamTest, GracefulShutdownSendsClose)
{
    scaler::wrapper::uv::Loop loop = UV_EXIT_ON_ERROR(scaler::wrapper::uv::Loop::init());
    TestTCPPair pair(loop);
    pair.waitForFullConnection();

    auto wsStream = scaler::ymq::internal::WebSocketStream::fromUpgradedSocket(
        std::move(pair._serverSide.value()), true /* server */);
    pair._serverSide.reset();
    pair._server.reset();

    std::vector<uint8_t> clientReceived;
    bool closeFrameReceived = false;

    UV_EXIT_ON_ERROR(
        pair._client->readStart([&](std::expected<std::span<const uint8_t>, scaler::wrapper::uv::Error> result) {
            if (!result.has_value())
                return;
            clientReceived.insert(clientReceived.end(), result->begin(), result->end());
            // Server CLOSE frame (unmasked): 0x88 (FIN=1, opcode=close), 0x00 (no payload).
            if (clientReceived.size() >= 2 && clientReceived[0] == 0x88)
                closeFrameReceived = true;
        }));

    UV_EXIT_ON_ERROR(wsStream.shutdown([](std::expected<void, scaler::wrapper::uv::Error>) {}));

    runUntil(loop, closeFrameReceived);

    EXPECT_TRUE(closeFrameReceived);
    ASSERT_GE(clientReceived.size(), 2u);
    EXPECT_EQ(clientReceived[0], 0x88);  // FIN=1, opcode=close
    EXPECT_EQ(clientReceived[1], 0x00);  // unmasked, no payload

    pair._client->readStop();
    UV_EXIT_ON_ERROR(pair._client->closeReset());
    loop.run(UV_RUN_DEFAULT);
}

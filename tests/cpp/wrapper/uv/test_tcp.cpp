#include <gtest/gtest.h>
#include <uv.h>

#include <expected>
#include <functional>
#include <memory>
#include <span>
#include <vector>

#include "scaler/wrapper/uv/callback.h"
#include "scaler/wrapper/uv/loop.h"
#include "scaler/wrapper/uv/socket_address.h"
#include "scaler/wrapper/uv/tcp.h"

class UVTCPTest: public ::testing::Test {
protected:
};

TEST_F(UVTCPTest, SocketAddress)
{
    // IPv4 address
    {
        auto ipv4 = UV_EXIT_ON_ERROR(scaler::wrapper::uv::SocketAddress::IPv4("192.168.1.12", 8080));

        std::string addressStr = UV_EXIT_ON_ERROR(ipv4.toString());
        ASSERT_EQ(addressStr, "192.168.1.12:8080");

        const sockaddr* sockAddr = ipv4.toSockAddr();
        ASSERT_NE(sockAddr, nullptr);

        ASSERT_FALSE(scaler::wrapper::uv::SocketAddress::IPv4("invalid.ipv4.address", 8080).has_value());
    }

    // IPv6 address
    {
        auto ipv6 = UV_EXIT_ON_ERROR(scaler::wrapper::uv::SocketAddress::IPv6("2001:db8::1234", 22));

        const std::string addressStr = UV_EXIT_ON_ERROR(ipv6.toString());
        ASSERT_EQ(addressStr, "2001:db8::1234:22");

        const sockaddr* sockAddr = ipv6.toSockAddr();
        ASSERT_NE(sockAddr, nullptr);

        ASSERT_FALSE(scaler::wrapper::uv::SocketAddress::IPv6("invalid.ipv6.address", 22).has_value());
    }
}

class TCPEchoServer {
public:
    TCPEchoServer(scaler::wrapper::uv::Loop& loop)
        : _loop(loop), _server(UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPServer::init(loop)))
    {
        scaler::wrapper::uv::SocketAddress address =
            UV_EXIT_ON_ERROR(scaler::wrapper::uv::SocketAddress::IPv4("127.0.0.1", 0));

        UV_EXIT_ON_ERROR(_server.bind(address, uv_tcp_flags(0)));
        UV_EXIT_ON_ERROR(_server.listen(16, std::bind_front(&TCPEchoServer::onClientConnected, this)));
    }

    scaler::wrapper::uv::SocketAddress address() const { return UV_EXIT_ON_ERROR(_server.getSockName()); }

private:
    scaler::wrapper::uv::Loop& _loop;
    scaler::wrapper::uv::TCPServer _server;

    void onClientConnected(std::expected<void, scaler::wrapper::uv::Error> result)
    {
        UV_EXIT_ON_ERROR(result);

        auto client = std::make_shared<scaler::wrapper::uv::TCPSocket>(
            std::move(UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPSocket::init(_loop))));
        UV_EXIT_ON_ERROR(_server.accept(*client));

        UV_EXIT_ON_ERROR(client->readStart(std::bind_front(onClientRead, client)));
    }

    static void onClientRead(
        std::shared_ptr<scaler::wrapper::uv::TCPSocket> client,
        std::expected<std::span<const uint8_t>, scaler::wrapper::uv::Error> readResult)
    {
        if (!readResult.has_value() && readResult.error() == scaler::wrapper::uv::Error {UV_EOF}) {
            // Client disconnected.
            client->readStop();
            return;
        }

        std::span<const uint8_t> readBuffer = UV_EXIT_ON_ERROR(readResult);

        // Copies the received buffer into a std::vector that will be shared with the write callback, to
        // ensure the written bytes will not be freed until the write completes.
        auto buffer = std::make_shared<const std::vector<uint8_t>>(readBuffer.cbegin(), readBuffer.cend());

        UV_EXIT_ON_ERROR(client->write(*buffer, [buffer](std::expected<void, scaler::wrapper::uv::Error> result) {
            UV_EXIT_ON_ERROR(std::move(result));
        }));
    }
};

TEST_F(UVTCPTest, EchoServer)
{
    const std::vector<uint8_t> message {'h', 'e', 'l', 'l', 'o'};

    scaler::wrapper::uv::Loop loop = UV_EXIT_ON_ERROR(scaler::wrapper::uv::Loop::init());

    TCPEchoServer server(loop);

    // Create a client and connect to the server

    scaler::wrapper::uv::TCPSocket client = UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPSocket::init(loop));
    bool responseReceived                 = false;

    auto onClientRead = [&](std::expected<std::span<const uint8_t>, scaler::wrapper::uv::Error> result) {
        std::span<const uint8_t> buffer = UV_EXIT_ON_ERROR(result);

        // Check if the received message matches the sent message
        ASSERT_TRUE(std::equal(buffer.begin(), buffer.end(), message.begin(), message.end()));

        responseReceived = true;
    };

    auto onClientConnected = [&](std::expected<void, scaler::wrapper::uv::Error> result) {
        UV_EXIT_ON_ERROR(result);

        UV_EXIT_ON_ERROR(client.getSockName());
        UV_EXIT_ON_ERROR(client.getPeerName());

        UV_EXIT_ON_ERROR(client.readStart(onClientRead));

        // Send the message to the server
        UV_EXIT_ON_ERROR(client.write(
            message, [](std::expected<void, scaler::wrapper::uv::Error>&& result) { UV_EXIT_ON_ERROR(result); }));
    };

    UV_EXIT_ON_ERROR(client.connect(server.address(), onClientConnected));

    // Loop until the echo response is received

    while (!responseReceived) {
        loop.run(UV_RUN_ONCE);
    }

    client.readStop();
}

TEST_F(UVTCPTest, CloseReset)
{
    scaler::wrapper::uv::Loop loop = UV_EXIT_ON_ERROR(scaler::wrapper::uv::Loop::init());

    // Create a server that track connection resets.

    scaler::wrapper::uv::TCPServer server = UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPServer::init(loop));
    scaler::wrapper::uv::SocketAddress serverAddress =
        UV_EXIT_ON_ERROR(scaler::wrapper::uv::SocketAddress::IPv4("127.0.0.1", 0));
    UV_EXIT_ON_ERROR(server.bind(serverAddress, uv_tcp_flags(0)));

    scaler::wrapper::uv::TCPSocket serverSocket = UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPSocket::init(loop));
    bool receivedConnReset                      = false;

    auto onServerRead = [&](std::expected<std::span<const uint8_t>, scaler::wrapper::uv::Error> result) {
        if (!result.has_value()) {
            if (result.error().code() == UV_ECONNRESET) {
                receivedConnReset = true;
            }
        }
    };

    auto onClientConnected = [&](std::expected<void, scaler::wrapper::uv::Error> result) {
        UV_EXIT_ON_ERROR(result);
        UV_EXIT_ON_ERROR(server.accept(serverSocket));
        UV_EXIT_ON_ERROR(serverSocket.readStart(onServerRead));
    };

    UV_EXIT_ON_ERROR(server.listen(16, onClientConnected));

    // Create a client and connect to the server

    scaler::wrapper::uv::TCPSocket client = UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPSocket::init(loop));
    bool connected                        = false;

    auto onClientConnect = [&](std::expected<void, scaler::wrapper::uv::Error> result) { connected = true; };

    UV_EXIT_ON_ERROR(client.connect(UV_EXIT_ON_ERROR(server.getSockName()), onClientConnect));

    // Loop until connected
    while (!connected) {
        loop.run(UV_RUN_ONCE);
    }

    // Call closeReset() to immediately close the connection with a RST segment

    UV_EXIT_ON_ERROR(client.closeReset());

    // Loop until the server receives the ECONNRESET error
    while (!receivedConnReset) {
        loop.run(UV_RUN_ONCE);
    }

    ASSERT_TRUE(receivedConnReset);
}

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
#include "tests/cpp/wrapper/uv/utility.h"

class UVTCPTest: public ::testing::Test {
protected:
};

TEST_F(UVTCPTest, SocketAddress)
{
    // IPv4 address
    {
        auto ipv4 = expectSuccess(scaler::wrapper::uv::SocketAddress::IPv4("192.168.1.12", 8080));

        std::string addressStr = expectSuccess(ipv4.toString());
        ASSERT_EQ(addressStr, "192.168.1.12:8080");

        const sockaddr* sockAddr = ipv4.toSockAddr();
        ASSERT_NE(sockAddr, nullptr);

        ASSERT_FALSE(scaler::wrapper::uv::SocketAddress::IPv4("invalid.ipv4.address", 8080).has_value());
    }

    // IPv6 address
    {
        auto ipv6 = expectSuccess(scaler::wrapper::uv::SocketAddress::IPv6("2001:db8::1234", 22));

        std::string addressStr = expectSuccess(ipv6.toString());
        ASSERT_EQ(addressStr, "2001:db8::1234:22");

        const sockaddr* sockAddr = ipv6.toSockAddr();
        ASSERT_NE(sockAddr, nullptr);

        ASSERT_FALSE(scaler::wrapper::uv::SocketAddress::IPv6("invalid.ipv6.address", 22).has_value());
    }
}

class TCPEchoServer {
public:
    TCPEchoServer(scaler::wrapper::uv::Loop& loop)
        : _loop(loop), _server(expectSuccess(scaler::wrapper::uv::TCPServer::init(loop)))
    {
        scaler::wrapper::uv::SocketAddress address =
            expectSuccess(scaler::wrapper::uv::SocketAddress::IPv4("127.0.0.1", 0));

        expectSuccess(_server.bind(address, uv_tcp_flags(0)));
        expectSuccess(_server.listen(16, std::bind_front(&TCPEchoServer::onClientConnected, this)));
    }

    scaler::wrapper::uv::SocketAddress address() const { return expectSuccess(_server.getSockName()); }

private:
    scaler::wrapper::uv::Loop& _loop;
    scaler::wrapper::uv::TCPServer _server;

    void onClientConnected(std::expected<void, scaler::wrapper::uv::Error> result)
    {
        expectSuccess(result);

        auto client = std::make_shared<scaler::wrapper::uv::TCPSocket>(
            std::move(expectSuccess(scaler::wrapper::uv::TCPSocket::init(_loop))));
        expectSuccess(_server.accept(*client));

        expectSuccess(client->readStart(std::bind_front(onClientRead, client)));
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

        std::span<const uint8_t> readBuffer = expectSuccess(readResult);

        // Copies the received buffer into a std::vector that will be shared with the write callback, to
        // ensure the written bytes will not be freed until the write completes.
        auto buffer = std::make_shared<const std::vector<uint8_t>>(readBuffer.cbegin(), readBuffer.cend());

        expectSuccess(client->write(*buffer, [buffer](std::expected<void, scaler::wrapper::uv::Error> result) {
            expectSuccess<void>(std::move(result));
        }));
    }
};

TEST_F(UVTCPTest, TCP)
{
    const std::vector<uint8_t> message {'h', 'e', 'l', 'l', 'o'};

    scaler::wrapper::uv::Loop loop = expectSuccess(scaler::wrapper::uv::Loop::init());

    TCPEchoServer server(loop);

    // Create a client and connect to the server

    scaler::wrapper::uv::TCPSocket client = expectSuccess(scaler::wrapper::uv::TCPSocket::init(loop));
    bool responseReceived                 = false;

    auto onClientRead = [&](std::expected<std::span<const uint8_t>, scaler::wrapper::uv::Error> result) {
        std::span<const uint8_t> buffer = expectSuccess(result);

        // Check if the received message matches the sent message
        ASSERT_TRUE(std::equal(buffer.begin(), buffer.end(), message.begin(), message.end()));

        responseReceived = true;
    };

    auto onClientConnected = [&](std::expected<void, scaler::wrapper::uv::Error> result) {
        expectSuccess(result);

        expectSuccess(client.getSockName());
        expectSuccess(client.getPeerName());

        expectSuccess(client.readStart(onClientRead));

        // Send the message to the server
        expectSuccess(client.write(message, &expectSuccess<void>));
    };

    expectSuccess(client.connect(server.address(), onClientConnected));

    // Loop until the echo response is received

    while (!responseReceived) {
        loop.run(UV_RUN_ONCE);
    }

    client.readStop();
}

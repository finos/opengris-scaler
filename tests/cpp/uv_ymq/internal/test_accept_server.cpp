#include <gtest/gtest.h>

#include <expected>
#include <vector>

#include "scaler/uv_ymq/address.h"
#include "scaler/uv_ymq/internal/accept_server.h"
#include "scaler/wrapper/uv/error.h"
#include "scaler/wrapper/uv/loop.h"
#include "scaler/wrapper/uv/socket_address.h"
#include "scaler/wrapper/uv/tcp.h"

class UVYMQAcceptServerTest: public ::testing::Test {};

TEST_F(UVYMQAcceptServerTest, AcceptServer)
{
    const auto listenAddress  = scaler::uv_ymq::Address::fromString("tcp://127.0.0.1:0").value();
    const size_t nConnections = 10;

    scaler::wrapper::uv::Loop loop = UV_EXIT_ON_ERROR(scaler::wrapper::uv::Loop::init());

    size_t nConnectionCount = 0;
    scaler::uv_ymq::internal::AcceptServer server(
        loop, listenAddress, [&](scaler::uv_ymq::Client client) { ++nConnectionCount; });

    // Get the actual bound address (since we used port 0)
    scaler::wrapper::uv::SocketAddress boundAddress = server.address().asTCP();

    // Test accepting incoming connections
    {
        std::vector<scaler::wrapper::uv::TCPSocket> clientSockets {};

        for (size_t i = 0; i < nConnections; ++i) {
            scaler::wrapper::uv::TCPSocket clientSocket = UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPSocket::init(loop));
            UV_EXIT_ON_ERROR(clientSocket.connect(
                boundAddress,
                [](std::expected<void, scaler::wrapper::uv::Error> result) { UV_EXIT_ON_ERROR(result); }));

            clientSockets.push_back(std::move(clientSocket));
        }

        while (nConnectionCount < nConnections) {
            loop.run(UV_RUN_ONCE);
        }

        ASSERT_EQ(nConnectionCount, nConnections);
    }

    // Should stop accepting connections after disconnect()
    {
        server.disconnect();

        bool clientConnectionCallbackCalled = false;

        scaler::wrapper::uv::TCPSocket clientSocket = UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPSocket::init(loop));
        UV_EXIT_ON_ERROR(
            clientSocket.connect(boundAddress, [&](std::expected<void, scaler::wrapper::uv::Error> result) {
                ASSERT_FALSE(result.has_value());
                clientConnectionCallbackCalled = true;
            }));

        while (!clientConnectionCallbackCalled) {
            loop.run(UV_RUN_ONCE);
        }

        loop.run(UV_RUN_ONCE);

        ASSERT_EQ(nConnectionCount, nConnections);
    }
}

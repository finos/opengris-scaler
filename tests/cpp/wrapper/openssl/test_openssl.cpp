#include <gtest/gtest.h>

#include <expected>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "scaler/wrapper/openssl/secure_server.h"
#include "scaler/wrapper/openssl/secure_socket.h"
#include "scaler/wrapper/openssl/ssl_context.h"
#include "scaler/wrapper/uv/callback.h"
#include "scaler/wrapper/uv/loop.h"
#include "scaler/wrapper/uv/socket_address.h"
#include "scaler/wrapper/uv/tcp.h"

static const std::string sampleCertPath       = "sample_cert.pem";
static const std::string samplePrivateKeyPath = "sample_private_key.pem";

class OpenSSLTest: public ::testing::Test {
protected:
    scaler::wrapper::openssl::SSLContext createServerContext()
    {
        auto context = scaler::wrapper::openssl::SSLContext::init().value();
        context.useCertificateChainFile(sampleCertPath).value();
        context.usePrivateKeyFile(samplePrivateKeyPath).value();
        context.checkPrivateKey().value();
        return context;
    }

    scaler::wrapper::openssl::SSLContext createClientContext()
    {
        return scaler::wrapper::openssl::SSLContext::init().value();
    }
};

TEST_F(OpenSSLTest, SSLContext)
{
    auto context = scaler::wrapper::openssl::SSLContext::init();
    ASSERT_TRUE(context.has_value());
    ASSERT_NE(context->native(), nullptr);

    ASSERT_TRUE(context->useCertificateChainFile(sampleCertPath).has_value());
    ASSERT_TRUE(context->usePrivateKeyFile(samplePrivateKeyPath).has_value());
    ASSERT_TRUE(context->checkPrivateKey().has_value());
}

TEST_F(OpenSSLTest, SSLContextInvalidPath)
{
    auto context = scaler::wrapper::openssl::SSLContext::init();
    ASSERT_TRUE(context.has_value());

    ASSERT_FALSE(context->useCertificateChainFile("/nonexistent/cert.pem").has_value());
    ASSERT_FALSE(context->usePrivateKeyFile("/nonexistent/key.pem").has_value());
}

TEST_F(OpenSSLTest, SecureSocketInit)
{
    scaler::wrapper::uv::Loop loop = scaler::wrapper::uv::Loop::init().value();

    auto context   = createClientContext();
    auto transport = scaler::wrapper::uv::TCPSocket::init(loop).value();
    auto socket    = scaler::wrapper::openssl::SecureSocket::init(std::move(context), std::move(transport));

    ASSERT_TRUE(socket.has_value());
    ASSERT_EQ(socket->state(), scaler::wrapper::openssl::SecureSocket::State::Uninitialized);
    ASSERT_FALSE(socket->established());
}

class TLSEchoServer {
public:
    TLSEchoServer(scaler::wrapper::uv::Loop& loop, scaler::wrapper::openssl::SSLContext context)
        : _loop(loop)
        , _context(std::move(context))
        , _server(UV_EXIT_ON_ERROR(scaler::wrapper::openssl::SecureServer::init(_context, loop)))
    {
        scaler::wrapper::uv::SocketAddress address =
            UV_EXIT_ON_ERROR(scaler::wrapper::uv::SocketAddress::IPv4("127.0.0.1", 0));

        UV_EXIT_ON_ERROR(_server.bind(address, uv_tcp_flags(0)));
        UV_EXIT_ON_ERROR(_server.listen(16, std::bind_front(&TLSEchoServer::onClientConnected, this)));
    }

    scaler::wrapper::uv::SocketAddress address() const
    {
        return UV_EXIT_ON_ERROR(_server.getSockName());
    }

    bool clientConnected() const noexcept
    {
        return _client.has_value();
    }

private:
    scaler::wrapper::uv::Loop& _loop;
    scaler::wrapper::openssl::SSLContext _context;
    scaler::wrapper::openssl::SecureServer _server;
    std::optional<scaler::wrapper::openssl::SecureSocket> _client {};

    void onClientConnected(std::expected<void, scaler::wrapper::uv::Error> result)
    {
        UV_EXIT_ON_ERROR(result);

        auto transport = UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPSocket::init(_loop));
        auto secureSocket =
            UV_EXIT_ON_ERROR(scaler::wrapper::openssl::SecureSocket::init(_context, std::move(transport)));

        _client.emplace(std::move(secureSocket));

        UV_EXIT_ON_ERROR(_server.accept(*_client));

        UV_EXIT_ON_ERROR(_client->readStart(std::bind_front(&TLSEchoServer::onClientRead, this)));
    }

    void onClientRead(std::expected<std::span<const uint8_t>, scaler::wrapper::uv::Error> readResult)
    {
        if (!readResult.has_value() && readResult.error() == scaler::wrapper::uv::Error {UV_EOF}) {
            _client->readStop();
            _client.reset();
            return;
        }

        std::span<const uint8_t> readBuffer = UV_EXIT_ON_ERROR(readResult);

        auto buffer = std::make_shared<const std::vector<uint8_t>>(readBuffer.begin(), readBuffer.end());

        UV_EXIT_ON_ERROR(_client->write(*buffer, [buffer](std::expected<void, scaler::wrapper::uv::Error> result) {
            UV_EXIT_ON_ERROR(std::move(result));
        }));
    }
};

TEST_F(OpenSSLTest, EchoServer)
{
    const std::vector<uint8_t> message {'h', 'e', 'l', 'l', 'o'};

    scaler::wrapper::uv::Loop loop = UV_EXIT_ON_ERROR(scaler::wrapper::uv::Loop::init());

    TLSEchoServer server(loop, createServerContext());

    // Create a TLS client and connect to the server

    auto clientContext                            = createClientContext();
    auto clientTransport                          = UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPSocket::init(loop));
    scaler::wrapper::openssl::SecureSocket client = UV_EXIT_ON_ERROR(
        scaler::wrapper::openssl::SecureSocket::init(std::move(clientContext), std::move(clientTransport)));

    bool responseReceived = false;

    auto onClientRead = [&](std::expected<std::span<const uint8_t>, scaler::wrapper::uv::Error> result) {
        std::span<const uint8_t> buffer = UV_EXIT_ON_ERROR(result);

        ASSERT_TRUE(std::equal(buffer.begin(), buffer.end(), message.begin(), message.end()));

        responseReceived = true;
    };

    auto onClientConnected = [&](std::expected<void, scaler::wrapper::uv::Error> result) {
        UV_EXIT_ON_ERROR(result);

        UV_EXIT_ON_ERROR(client.readStart(onClientRead));

        UV_EXIT_ON_ERROR(client.write(
            message, [](std::expected<void, scaler::wrapper::uv::Error>&& result) { UV_EXIT_ON_ERROR(result); }));
    };

    UV_EXIT_ON_ERROR(client.connect(server.address(), onClientConnected));

    // Loop until the echo response is received

    while (!responseReceived) {
        loop.run(UV_RUN_ONCE);
    }

    client.readStop();
    ASSERT_TRUE(server.clientConnected());

    // Shutdown the client and verify the server sees the disconnect

    UV_EXIT_ON_ERROR(client.shutdown(
        [&](std::expected<void, scaler::wrapper::uv::Error> shutdownResult) { UV_EXIT_ON_ERROR(shutdownResult); }));

    while (server.clientConnected()) {
        loop.run(UV_RUN_ONCE);
    }
}

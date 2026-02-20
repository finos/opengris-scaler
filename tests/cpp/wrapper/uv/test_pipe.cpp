#include <gtest/gtest.h>
#include <uv.h>

#include <cstdlib>
#include <ctime>
#include <expected>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "scaler/wrapper/uv/callback.h"
#include "scaler/wrapper/uv/loop.h"
#include "scaler/wrapper/uv/pipe.h"

// Platform-specific pipe name format
// Defined in test_pipe_unix.cpp and test_pipe_windows.cpp
extern const char* pipeNamePrefix;

class UVPipeTest: public ::testing::Test {
protected:
};

class PipeEchoServer {
public:
    PipeEchoServer(scaler::wrapper::uv::Loop& loop, const std::string& pipeName)
        : _loop(loop), _server(UV_EXIT_ON_ERROR(scaler::wrapper::uv::PipeServer::init(loop, false)))
    {
        // Bind to a pipe name
        UV_EXIT_ON_ERROR(_server.bind(pipeName));
        UV_EXIT_ON_ERROR(_server.listen(16, std::bind_front(&PipeEchoServer::onClientConnected, this)));
    }

    std::string pipeName() const { return UV_EXIT_ON_ERROR(_server.getSockName()); }

private:
    scaler::wrapper::uv::Loop& _loop;
    scaler::wrapper::uv::PipeServer _server;

    void onClientConnected(std::expected<void, scaler::wrapper::uv::Error>&& result)
    {
        UV_EXIT_ON_ERROR(result);

        auto client = std::make_shared<scaler::wrapper::uv::Pipe>(
            std::move(UV_EXIT_ON_ERROR(scaler::wrapper::uv::Pipe::init(_loop, false))));
        UV_EXIT_ON_ERROR(_server.accept(*client));

        UV_EXIT_ON_ERROR(client->readStart(std::bind_front(onClientRead, client)));
    }

    static void onClientRead(
        std::shared_ptr<scaler::wrapper::uv::Pipe> client,
        std::expected<std::span<const uint8_t>, scaler::wrapper::uv::Error>&& readResult)
    {
        if (!readResult.has_value() && readResult.error() == scaler::wrapper::uv::Error {UV_EOF}) {
            // Client disconnected.
            client->readStop();
            return;
        }

        std::span<const uint8_t> readBuffer = UV_EXIT_ON_ERROR(readResult);

        // Echo the received data back to the client
        auto buffer = std::make_shared<const std::vector<uint8_t>>(readBuffer.cbegin(), readBuffer.cend());
        UV_EXIT_ON_ERROR(client->write(
            std::span<const uint8_t>(*buffer), [buffer](std::expected<void, scaler::wrapper::uv::Error>&& result) {
                UV_EXIT_ON_ERROR(std::move(result));
            }));
    }
};

TEST_F(UVPipeTest, Pipe)
{
    const std::vector<uint8_t> message {'h', 'e', 'l', 'l', 'o'};

    const std::string pipeName =
        std::string(pipeNamePrefix) + "scaler_test_pipe_" + std::to_string(getpid()) + "_" + std::to_string(rand());

    scaler::wrapper::uv::Loop loop = UV_EXIT_ON_ERROR(scaler::wrapper::uv::Loop::init());

    // Create echo server
    PipeEchoServer server(loop, pipeName);
    ASSERT_EQ(pipeName, server.pipeName());

    // Create client and connect to the server
    scaler::wrapper::uv::Pipe client = UV_EXIT_ON_ERROR(scaler::wrapper::uv::Pipe::init(loop, false));
    bool responseReceived            = false;

    auto onClientRead = [&](std::expected<std::span<const uint8_t>, scaler::wrapper::uv::Error>&& result) {
        std::span<const uint8_t> buffer = UV_EXIT_ON_ERROR(result);

        // Check if the received message matches the sent message
        ASSERT_TRUE(std::equal(buffer.begin(), buffer.end(), message.begin(), message.end()));

        responseReceived = true;
    };

    auto onClientConnected = [&](std::expected<void, scaler::wrapper::uv::Error>&& result) {
        UV_EXIT_ON_ERROR(result);

        ASSERT_EQ(pipeName, client.getPeerName());

        UV_EXIT_ON_ERROR(client.readStart(onClientRead));

        // Send the message to the server
        UV_EXIT_ON_ERROR(client.write(
            message, [](std::expected<void, scaler::wrapper::uv::Error>&& result) { UV_EXIT_ON_ERROR(result); }));
    };

    // Connect to the server
    UV_EXIT_ON_ERROR(client.connect(pipeName, onClientConnected));

    // Loop until the echo response is received
    while (!responseReceived) {
        loop.run(UV_RUN_ONCE);
    }

    client.readStop();
}

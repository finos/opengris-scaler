// TCP echo server using scaler::uv
//
// This is the C++ equivalent of libuv's tcp-echo-server example:
// https://github.com/libuv/libuv/blob/v1.x/docs/code/tcp-echo-server/main.c

#include <functional>
#include <iostream>
#include <memory>
#include <span>
#include <vector>

#include "scaler/wrapper/uv/error.h"
#include "scaler/wrapper/uv/loop.h"
#include "scaler/wrapper/uv/socket_address.h"
#include "scaler/wrapper/uv/tcp.h"

const int DEFAULT_BACKLOG = 128;

class TCPEchoServer {
public:
    TCPEchoServer(scaler::wrapper::uv::Loop& loop, const scaler::wrapper::uv::SocketAddress& address)
        : _loop(loop), _server(UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPServer::init(loop)))
    {
        UV_EXIT_ON_ERROR(_server.bind(address, uv_tcp_flags(0)));
        UV_EXIT_ON_ERROR(_server.listen(DEFAULT_BACKLOG, std::bind_front(&TCPEchoServer::onNewConnection, this)));
    }

    scaler::wrapper::uv::SocketAddress address() { return UV_EXIT_ON_ERROR(_server.getSockName()); }

private:
    scaler::wrapper::uv::Loop& _loop;
    scaler::wrapper::uv::TCPServer _server;

    void onNewConnection(std::expected<void, scaler::wrapper::uv::Error> result)
    {
        auto client = std::make_shared<scaler::wrapper::uv::TCPSocket>(
            UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPSocket::init(_loop)));
        UV_EXIT_ON_ERROR(_server.accept(*client));

        UV_EXIT_ON_ERROR(client->readStart(std::bind_front(echoRead, client)));
    }

    static void echoRead(
        std::shared_ptr<scaler::wrapper::uv::TCPSocket> client,
        std::expected<std::span<const uint8_t>, scaler::wrapper::uv::Error> readResult)
    {
        if (!readResult.has_value() && readResult.error() == scaler::wrapper::uv::Error {UV_EOF}) {
            // Client disconnecting
            return;
        }

        std::span<const uint8_t> readBuffer = readResult.value();

        // Copies the received buffer into a std::vector that will be shared with the write callback, to
        // ensure the written bytes will not be freed until the write completes.
        auto buffer = std::make_shared<const std::vector<uint8_t>>(readBuffer.cbegin(), readBuffer.cend());

        UV_EXIT_ON_ERROR(client->write(*buffer, [buffer](std::expected<void, scaler::wrapper::uv::Error> writeResult) {
            UV_EXIT_ON_ERROR(std::move(writeResult));
        }));
    }
};

int main()
{
    // Initialize the event loop
    scaler::wrapper::uv::Loop loop = UV_EXIT_ON_ERROR(scaler::wrapper::uv::Loop::init());

    // Initialize the echo server
    TCPEchoServer server(loop, UV_EXIT_ON_ERROR(scaler::wrapper::uv::SocketAddress::IPv4("0.0.0.0", 0)));

    std::cout << "TCP echo server listening on " << UV_EXIT_ON_ERROR(server.address().toString()) << "\n";

    // Run the event loop
    loop.run(UV_RUN_DEFAULT);

    return 0;
}

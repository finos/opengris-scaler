// TCP echo server using scaler::uv
//
// This is the C++ equivalent of libuv's tcp-echo-server example:
// https://github.com/libuv/libuv/blob/v1.x/docs/code/tcp-echo-server/main.c

#include <functional>
#include <iostream>
#include <memory>
#include <span>
#include <vector>

#include "scaler/uv/error.h"
#include "scaler/uv/loop.h"
#include "scaler/uv/socket_address.h"
#include "scaler/uv/tcp.h"
#include "utility.h"  // exitOnFailure

using namespace scaler;

const int DEFAULT_BACKLOG = 128;

class TCPEchoServer {
public:
    TCPEchoServer(uv::Loop& loop, const uv::SocketAddress& address)
        : loop_(loop), server_(exitOnFailure(uv::TCPServer::init(loop)))
    {
        exitOnFailure(server_.bind(address, uv_tcp_flags(0)));
        exitOnFailure(server_.listen(DEFAULT_BACKLOG, std::bind_front(&TCPEchoServer::onNewConnection, this)));
    }

    uv::SocketAddress address() { return exitOnFailure(server_.getSockName()); }

private:
    uv::Loop& loop_;
    uv::TCPServer server_;

    void onNewConnection(std::expected<void, uv::Error>&& result)
    {
        auto client = std::make_shared<uv::TCPSocket>(exitOnFailure(uv::TCPSocket::init(loop_)));
        exitOnFailure(server_.accept(*client));

        exitOnFailure(client->readStart(std::bind_front(echoRead, client)));
    }

    static void echoRead(
        std::shared_ptr<uv::TCPSocket> client, std::expected<std::span<uint8_t>, uv::Error>&& readResult)
    {
        if (!readResult.has_value() && readResult.error() == uv::Error {UV_EOF}) {
            // Client disconnecting
            return;
        }

        std::span<uint8_t> readBuffer = readResult.value();

        // Copies the received buffer into a std::vector that will be shared with the write callback, to
        // ensure the written bytes will not be freed until the write completes.
        auto buffer = std::make_shared<std::vector<uint8_t>>(readBuffer.cbegin(), readBuffer.cend());

        exitOnFailure(client->write(
            *buffer, [buffer](std::expected<void, uv::Error> writeResult) { exitOnFailure(std::move(writeResult)); }));
    }
};

int main()
{
    // Initialize the event loop
    uv::Loop loop = exitOnFailure(uv::Loop::init());

    // Initialize the echo server
    TCPEchoServer server(loop, exitOnFailure(uv::SocketAddress::IPv4("0.0.0.0", 0)));

    std::cout << "TCP echo server listening on " << exitOnFailure(server.address().toString()) << "\n";

    // Run the event loop
    loop.run(UV_RUN_DEFAULT);

    return 0;
}

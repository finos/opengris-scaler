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
#include "utility.h"

using namespace scaler;

const int DEFAULT_PORT    = 7000;
const int DEFAULT_BACKLOG = 128;

class TCPEchoServer {
public:
    TCPEchoServer(uv::Loop& loop, const uv::SocketAddress& address)
        : loop_(loop), server_(exitOnFailure(uv::TCPServer::init(loop)))
    {
        exitOnFailure(server_.bind(address, uv_tcp_flags(0)));
        exitOnFailure(server_.listen(DEFAULT_BACKLOG, std::bind_front(&TCPEchoServer::onNewConnection, this)));
    }

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

        // Copy the buffer to ensure it lives until the write operation completes
        std::vector<uint8_t> writeBuffer(readBuffer.begin(), readBuffer.end());

        exitOnFailure(client->write(
            writeBuffer, [writeBuffer = std::move(writeBuffer)](std::expected<void, uv::Error> writeResult) {
                exitOnFailure(std::move(writeResult));
            }));
    }
};

int main()
{
    // Initialize the event loop
    uv::Loop loop = exitOnFailure(uv::Loop::init());

    // Create server address
    uv::SocketAddress address = exitOnFailure(uv::SocketAddress::IPv4("0.0.0.0", DEFAULT_PORT));

    std::cout << "TCP echo server listening on " << exitOnFailure(address.toString()) << "\n";

    // Create and run the echo server
    TCPEchoServer server(loop, address);

    // Run the event loop
    loop.run(UV_RUN_DEFAULT);

    return 0;
}

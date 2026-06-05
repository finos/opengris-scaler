#include <iostream>
#include <string>

#include "scaler/ymq/io_context.h"
#include "scaler/ymq/sync/connector_socket.h"

using scaler::ymq::Bytes;
using scaler::ymq::IOContext;
using scaler::ymq::sync::ConnectorSocket;

int main()
{
    std::string longStr = "1234567890";
    for (int i = 0; i < 400; ++i)
        longStr += "1234567890";

    IOContext context;

    auto result = ConnectorSocket::connect(context, "ClientSocket", "tcp://127.0.0.1:8080");
    if (!result.has_value()) {
        std::cerr << "Failed to connect: " << result.error().what() << std::endl;
        return 1;
    }

    auto socket = std::move(result.value());
    std::cout << "Connected to server.\n";

    constexpr size_t msgCnt = 100'000;

    for (size_t cnt = 0; cnt < msgCnt; ++cnt) {
        auto sendResult = socket.sendMessage(Bytes {longStr});
        if (!sendResult.has_value()) {
            std::cerr << "Failed to send message: " << sendResult.error().what() << std::endl;
            return 1;
        }

        auto recvResult = socket.recvMessage();
        if (!recvResult.has_value()) {
            std::cerr << "Failed to receive message: " << sendResult.error().what() << std::endl;
            return 1;
        }

        auto msg = std::move(recvResult.value());
        if (msg.payload.as_string() != longStr) {
            std::cerr << "Received message mismatch, got " << msg.payload.as_string().value_or("") << std::endl;
            return 1;
        }
    }

    std::cout << "Send and recv " << msgCnt << " messages, checksum fits, exiting.\n";

    return 0;
}

#include <iostream>
#include <string>

#include "scaler/ymq/io_context.h"
#include "scaler/ymq/sync/connector_socket.h"

using scaler::ymq::Bytes;
using scaler::ymq::IOContext;
using scaler::ymq::Message;
using scaler::ymq::sync::ConnectorSocket;

int main()
{
    IOContext context;

    auto result = ConnectorSocket::connect(context, "ClientSocket", "ipc:///tmp/uds_echo2.sock");
    if (!result.has_value()) {
        std::cerr << "Failed to connect: " << result.error().what() << std::endl;
        return 1;
    }

    auto socket = std::move(result.value());
    std::cout << "Connected to server.\n";

    for (int cnt = 0; cnt < 10; ++cnt) {
        std::string line;
        std::cout << "Enter a message to send: ";
        if (!std::getline(std::cin, line)) {
            std::cout << "EOF or input error. Exiting...\n";
            break;
        }
        std::cout << "YOU ENTERED THIS MESSAGE: " << line << std::endl;

        auto sendResult = socket.sendMessage(Bytes {line});
        if (!sendResult.has_value()) {
            std::cerr << "Failed to send message: " << sendResult.error().what() << std::endl;
            continue;
        }

        std::cout << "Message sent, waiting for response...\n";

        auto recvResult = socket.recvMessage();
        if (!recvResult.has_value()) {
            std::cerr << "Failed to receive message: " << recvResult.error().what() << std::endl;
            continue;
        }

        Message reply        = std::move(recvResult.value());
        std::string replyStr = reply.payload.as_string().value_or("");
        std::cout << "Received echo: '" << replyStr << "'.\n";
    }

    return 0;
}

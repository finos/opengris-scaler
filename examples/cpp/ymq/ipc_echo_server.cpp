#include <iostream>
#include <string>

#include "scaler/ymq/io_context.h"
#include "scaler/ymq/sync/binder_socket.h"

using scaler::ymq::Bytes;
using scaler::ymq::IOContext;
using scaler::ymq::Message;
using scaler::ymq::sync::BinderSocket;

int main()
{
    IOContext context;

    BinderSocket socket {context, "ServerSocket"};

    auto bindResult = socket.bindTo("ipc:///tmp/uds_echo2.sock");
    if (!bindResult.has_value()) {
        std::cerr << "Failed to bind socket: " << bindResult.error().what() << std::endl;
        return 1;
    }

    std::cout << "Successfully bound socket to " << bindResult->toString().value() << std::endl;

    while (true) {
        auto recvResult = socket.recvMessage();
        if (!recvResult.has_value()) {
            std::cerr << "Failed to receive message: " << recvResult.error().what() << std::endl;
            continue;
        }

        Message receivedMsg = std::move(recvResult.value());

        auto sendResult = socket.sendMessage(receivedMsg.address.as_string().value(), std::move(receivedMsg.payload));
        if (!sendResult.has_value()) {
            std::cerr << "Failed to send message: " << sendResult.error().what() << std::endl;
        }
    }

    return 0;
}

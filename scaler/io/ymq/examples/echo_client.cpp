
// C++
#include <stdio.h>
#include <unistd.h>

#include <future>
#include <iostream>
#include <memory>
#include <string>

#include "scaler/io/ymq/io_context.h"
#include "scaler/io/ymq/io_socket.h"
#include "scaler/io/ymq/typedefs.h"

int main() {
    IOContext context;

    auto createSocketPromise               = std::make_shared<std::promise<void>>();
    auto createSocketFuture                = createSocketPromise->get_future();
    std::shared_ptr<IOSocket> clientSocket = context.createIOSocket(
        "ServerSocket", IOSocketType::Dealer, [createSocketPromise] { createSocketPromise->set_value(); });

    createSocketFuture.wait();
    printf("Successfully created socket.\n");

    auto connect_promise = std::make_shared<std::promise<void>>();
    auto connect_future  = connect_promise->get_future();

    clientSocket->connectTo("tcp://127.0.0.1:8080", [connect_promise](int result) { connect_promise->set_value(); });

    printf("Waiting for connection...\n");
    connect_future.wait();
    printf("Connected to server.\n");

    int cnt = 0;
    while (cnt++ < 10) {
        std::string line;
        std::cout << "Enter a message to send: ";
        if (!std::getline(std::cin, line)) {
            std::cout << "EOF or input error. Exiting...\n";
            break;
        }
        std::cout << "YOU ENTERED THIS MESSAGE: " << line << std::endl;

        Message message;
        std::string destAddress = "ServerSocket";

        message.address = Bytes {const_cast<char*>(destAddress.c_str()), destAddress.size(), Ownership::Borrowed};

        message.payload = Bytes {const_cast<char*>(line.c_str()), line.size(), Ownership::Borrowed};

        auto send_promise = std::make_shared<std::promise<void>>();
        auto send_future  = send_promise->get_future();

        clientSocket->sendMessage(std::move(message), [send_promise](int) { send_promise->set_value(); });

        send_future.wait();
        printf("Message sent, waiting for response...\n");

        auto recv_promise = std::make_shared<std::promise<Message>>();
        auto recv_future  = recv_promise->get_future();

        clientSocket->recvMessage([recv_promise](Message msg) { recv_promise->set_value(std::move(msg)); });

        Message reply = recv_future.get();
        std::string reply_str(reply.payload.data(), reply.payload.data() + reply.payload.len());
        printf("Received echo: '%s'\n", reply_str.c_str());
    }

    context.removeIOSocket(clientSocket);

    return 0;
}

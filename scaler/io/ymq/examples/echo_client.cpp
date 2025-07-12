
// C++
#include <stdio.h>
#include <unistd.h>

#include <future>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include "scaler/io/ymq/io_context.h"
#include "scaler/io/ymq/io_socket.h"
#include "scaler/io/ymq/typedefs.h"

int main() {
    IOContext context;

    auto createSocketPromise               = std::promise<void>();
    auto createSocketFuture                = createSocketPromise.get_future();
    std::shared_ptr<IOSocket> clientSocket = context.createIOSocket(
        "ClientSocket", IOSocketType::Connector, [&createSocketPromise] { createSocketPromise.set_value(); });

    createSocketFuture.wait();
    printf("Successfully created socket.\n");

    auto connect_promise = std::promise<void>();
    auto connect_future  = connect_promise.get_future();

    clientSocket->connectTo("tcp://127.0.0.1:8080", [&connect_promise](int result) { connect_promise.set_value(); });

    printf("Waiting for connection...\n");
    connect_future.wait();
    printf("Connected to server.\n");

    for (int cnt = 0; cnt < 10; ++cnt) {
        std::string line;
        std::cout << "Enter a message to send: ";
        if (!std::getline(std::cin, line)) {
            std::cout << "EOF or input error. Exiting...\n";
            break;
        }
        std::cout << "YOU ENTERED THIS MESSAGE: " << line << std::endl;

        Message message;
        std::string destAddress = "ServerSocket";

        message.address = Bytes {const_cast<char*>(destAddress.c_str()), destAddress.size()};

        message.payload = Bytes {const_cast<char*>(line.c_str()), line.size()};

        auto send_promise = std::promise<void>();
        auto send_future  = send_promise.get_future();

        clientSocket->sendMessage(std::move(message), [&send_promise](int) { send_promise.set_value(); });

        send_future.wait();
        printf("Message sent, waiting for response...\n");

        auto recv_promise = std::promise<Message>();
        auto recv_future  = recv_promise.get_future();

        clientSocket->recvMessage([&recv_promise](Message msg) { recv_promise.set_value(std::move(msg)); });

        Message reply = recv_future.get();
        std::string reply_str(reply.payload.data(), reply.payload.data() + reply.payload.len());
        printf("Received echo: '%s'\n", reply_str.c_str());
    }

    // TODO: remove IOSocket also needs a future
    context.removeIOSocket(clientSocket);

    using namespace std::chrono_literals;
    std::this_thread::sleep_for(100ms);

    return 0;
}

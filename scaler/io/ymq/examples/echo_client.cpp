
// C++
#include <stdio.h>
#include <unistd.h>

#include <future>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include "./common.h"
#include "scaler/io/ymq/error.h"
#include "scaler/io/ymq/io_context.h"
#include "scaler/io/ymq/io_socket.h"
#include "scaler/io/ymq/typedefs.h"

using namespace scaler::ymq;

int main() {
    IOContext context;

    auto clientSocket = syncCreateSocket(context, IOSocketType::Connector, "ClientSocket");
    printf("Successfully created socket.\n");

    syncConnectSocket(clientSocket, "tcp://127.0.0.1:8080");
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

        message.address = Bytes {const_cast<char*>(destAddress.data()), destAddress.size()};
        message.payload = Bytes {const_cast<char*>(line.c_str()), line.size()};

        auto send_promise = std::promise<std::expected<void, Error>>();
        auto send_future  = send_promise.get_future();

        clientSocket->sendMessage(
            std::move(message), [&send_promise](std::expected<void, Error>) { send_promise.set_value({}); });

        send_future.wait();
        printf("Message sent, waiting for response...\n");

        auto recv_promise = std::promise<std::pair<Message, Error>>();
        auto recv_future  = recv_promise.get_future();

        clientSocket->recvMessage(
            [&recv_promise](std::pair<Message, Error> msg) { recv_promise.set_value(std::move(msg)); });

        Message reply = recv_future.get().first;
        std::string reply_str(reply.payload.data(), reply.payload.data() + reply.payload.len());
        printf("Received echo: '%s'\n", reply_str.c_str());
    }

    // TODO: remove IOSocket also needs a future
    context.removeIOSocket(clientSocket);

    using namespace std::chrono_literals;
    std::this_thread::sleep_for(100ms);

    return 0;
}

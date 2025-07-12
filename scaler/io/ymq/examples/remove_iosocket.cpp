

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

    context.removeIOSocket(clientSocket);

    return 0;
}

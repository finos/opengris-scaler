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

std::shared_ptr<IOSocket> syncCreateSocket(IOContext& context, std::string name) {
    auto createSocketPromise = std::promise<void>();
    auto createSocketFuture  = createSocketPromise.get_future();
    auto clientSocket        = context.createIOSocket(
        name, IOSocketType::Unicast, [&createSocketPromise] { createSocketPromise.set_value(); });
    createSocketFuture.wait();
    return clientSocket;
}

void syncConnectToRemote(std::shared_ptr<IOSocket> socket) {
    auto connect_promise = std::promise<void>();
    auto connect_future  = connect_promise.get_future();

    socket->connectTo("tcp://127.0.0.1:8080", [&connect_promise](int result) { connect_promise.set_value(); });

    printf("Waiting for connection...\n");
    connect_future.wait();
    printf("Connected to server.\n");
}

int main() {
    IOContext context;
    auto clientSocket1 = syncCreateSocket(context, "ClientSocket1");
    auto clientSocket2 = syncCreateSocket(context, "ClientSocket2");
    printf("Successfully created sockets.\n");

    syncConnectToRemote(clientSocket1);
    syncConnectToRemote(clientSocket2);

    for (int cnt = 0; cnt < 10; ++cnt) {
        auto recv_promise = std::promise<Message>();
        auto recv_future  = recv_promise.get_future();

        clientSocket1->recvMessage([&recv_promise](Message msg) { recv_promise.set_value(std::move(msg)); });

        Message reply = recv_future.get();
        std::string reply_str(reply.payload.data(), reply.payload.data() + reply.payload.len());
        printf("clientSocket1 Received from publisher: '%s'\n", reply_str.c_str());

        auto recv_promise2 = std::promise<Message>();
        auto recv_future2  = recv_promise2.get_future();

        clientSocket2->recvMessage([&recv_promise2](Message msg) { recv_promise2.set_value(std::move(msg)); });

        Message reply2 = recv_future2.get();
        std::string reply_str2(reply2.payload.data(), reply2.payload.data() + reply2.payload.len());
        printf("clientSocket2 Received from publisher: '%s'\n", reply_str2.c_str());
    }

    // TODO: remove IOSocket also needs a future
    context.removeIOSocket(clientSocket1);
    context.removeIOSocket(clientSocket2);

    return 0;
}

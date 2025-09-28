#include <stdio.h>

#include <future>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include "scaler/io/ymq/error.h"
#include "scaler/io/ymq/io_context.h"
#include "scaler/io/ymq/io_socket.h"
#include "scaler/io/ymq/simple_interface.h"
#include "scaler/io/ymq/typedefs.h"

using namespace scaler::ymq;

int main()
{
    IOContext context;
    auto clientSocket1 = syncCreateSocket(context, IOSocketType::Unicast, "ClientSocket1");
    auto clientSocket2 = syncCreateSocket(context, IOSocketType::Unicast, "ClientSocket2");
    printf("Successfully created sockets.\n");

    syncConnectSocket(clientSocket1, "tcp://127.0.0.1:8080");
    syncConnectSocket(clientSocket2, "tcp://127.0.0.1:8080");

    for (int cnt = 0; cnt < 10; ++cnt) {
        auto recv_promise = std::promise<std::pair<Message, Error>>();
        auto recv_future  = recv_promise.get_future();

        clientSocket1->recvMessage(
            [&recv_promise](std::pair<Message, Error> msg) { recv_promise.set_value(std::move(msg)); });

        Message reply = recv_future.get().first;
        std::string reply_str(reply.payload.data(), reply.payload.data() + reply.payload.len());
        printf("clientSocket1 Received from publisher: '%s'\n", reply_str.c_str());

        auto recv_promise2 = std::promise<std::pair<Message, Error>>();
        auto recv_future2  = recv_promise2.get_future();

        clientSocket2->recvMessage(
            [&recv_promise2](std::pair<Message, Error> msg) { recv_promise2.set_value(std::move(msg)); });

        Message reply2 = recv_future2.get().first;
        std::string reply_str2(reply2.payload.data(), reply2.payload.data() + reply2.payload.len());
        printf("clientSocket2 Received from publisher: '%s'\n", reply_str2.c_str());
    }

    // TODO: remove IOSocket also needs a future
    context.removeIOSocket(clientSocket1);
    context.removeIOSocket(clientSocket2);

    return 0;
}

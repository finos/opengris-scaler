#include <future>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include "scaler/error/error.h"
#include "scaler/ymq/io_context.h"
#include "scaler/ymq/io_socket.h"
#include "scaler/ymq/simple_interface.h"
#include "scaler/ymq/typedefs.h"

using namespace scaler::ymq;

int main()
{
    IOContext context;
    auto clientSocket1 = syncCreateSocket(context, IOSocketType::Unicast, "ClientSocket1");
    auto clientSocket2 = syncCreateSocket(context, IOSocketType::Unicast, "ClientSocket2");
    std::cout << "Successfully created sockets.\n";

    syncConnectSocket(clientSocket1, "tcp://127.0.0.1:8080");
    syncConnectSocket(clientSocket2, "tcp://127.0.0.1:8080");

    for (int cnt = 0; cnt < 10; ++cnt) {
        auto recv_promise = std::promise<std::pair<Message, Error>>();
        auto recv_future  = recv_promise.get_future();

        clientSocket1->recvMessage(
            [&recv_promise](std::pair<Message, Error> msg) { recv_promise.set_value(std::move(msg)); });

        Message reply = recv_future.get().first;
        std::string reply_str(reply.payload.data(), reply.payload.data() + reply.payload.len());
        std::cout << "clientSocket1 Received from publisher: '" << reply_str << "'.\n";

        auto recv_promise2 = std::promise<std::pair<Message, Error>>();
        auto recv_future2  = recv_promise2.get_future();

        clientSocket2->recvMessage(
            [&recv_promise2](std::pair<Message, Error> msg) { recv_promise2.set_value(std::move(msg)); });

        Message reply2 = recv_future2.get().first;
        std::string reply_str2(reply2.payload.data(), reply2.payload.data() + reply2.payload.len());
        std::cout << "clientSocket2 Received from publisher: '" << reply_str2 << "'.\n";
    }

    // TODO: remove IOSocket also needs a future
    context.removeIOSocket(clientSocket1);
    context.removeIOSocket(clientSocket2);

    return 0;
}

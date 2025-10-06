
#include <future>
#include <iostream>
#include <memory>

#include "scaler/io/ymq/error.h"
#include "scaler/io/ymq/io_context.h"
#include "scaler/io/ymq/io_socket.h"
#include "scaler/io/ymq/simple_interface.h"

using namespace scaler::ymq;

int main()
{
    IOContext context;

    auto socket = syncCreateSocket(context, IOSocketType::Binder, "ServerSocket");
    std::cout << "Successfully created socket." << std::endl;

    syncBindSocket(socket, "tcp://127.0.0.1:8080");
    std::cout << "Successfully bound socket." << std::endl;

    while (true) {
        auto recv_promise = std::promise<std::pair<Message, Error>>();
        auto recv_future  = recv_promise.get_future();

        socket->recvMessage([&recv_promise](std::pair<Message, Error> msg) { recv_promise.set_value(std::move(msg)); });

        Message received_msg = recv_future.get().first;
        auto send_promise    = std::promise<std::expected<void, Error>>();
        auto send_future     = send_promise.get_future();
        socket->sendMessage(
            std::move(received_msg), [&send_promise](std::expected<void, Error>) { send_promise.set_value({}); });
        send_future.wait();
    }

    return 0;
}

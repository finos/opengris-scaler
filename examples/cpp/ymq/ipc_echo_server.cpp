

#include <future>
#include <iostream>
#include <memory>

#include "scaler/error/error.h"
#include "scaler/ymq/io_context.h"
#include "scaler/ymq/io_socket.h"
#include "scaler/ymq/simple_interface.h"

using scaler::ymq::Error;
using scaler::ymq::IOContext;
using scaler::ymq::IOSocketType;
using scaler::ymq::Message;
using scaler::ymq::syncBindSocket;
using scaler::ymq::syncCreateSocket;

int main()
{
    IOContext context;

    auto socket = syncCreateSocket(context, IOSocketType::Binder, "ServerSocket");
    std::cout << "Successfully created socket." << std::endl;

    syncBindSocket(socket, "ipc:///tmp/uds_echo2.sock");
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

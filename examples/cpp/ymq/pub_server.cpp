#include <chrono>
#include <future>
#include <iostream>
#include <memory>
#include <thread>

#include "scaler/error/error.h"
#include "scaler/ymq/io_context.h"
#include "scaler/ymq/io_socket.h"
#include "scaler/ymq/simple_interface.h"

using scaler::ymq::Bytes;
using scaler::ymq::Error;
using scaler::ymq::IOContext;
using scaler::ymq::IOSocketType;
using scaler::ymq::Message;
using scaler::ymq::syncBindSocket;
using scaler::ymq::syncCreateSocket;

int main()
{
    IOContext context;

    auto socket = syncCreateSocket(context, IOSocketType::Multicast, "ServerSocket");
    std::cout << "Successfully created socket.\n";

    syncBindSocket(socket, "tcp://127.0.0.1:8080");
    std::cout << "Successfully bound socket.\n";

    while (true) {
        std::string address("");
        std::string payload("Hello from the publisher");

        Message publishContent;
        publishContent.address = Bytes(address.data(), address.size());
        publishContent.payload = Bytes(payload.data(), payload.size());

        auto send_promise = std::promise<std::expected<void, Error>>();
        auto send_future  = send_promise.get_future();
        socket->sendMessage(
            std::move(publishContent), [&send_promise](std::expected<void, Error>) { send_promise.set_value({}); });
        send_future.wait();

        std::cout << "One message published, sleep for 10 sec.\n";
        std::this_thread::sleep_for(std::chrono::seconds(10));
    }

    return 0;
}

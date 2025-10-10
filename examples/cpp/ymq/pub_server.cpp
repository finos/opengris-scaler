#include <chrono>
#include <future>
#include <iostream>
#include <memory>
#include <thread>

#include "scaler/io/ymq/error.h"
#include "scaler/io/ymq/io_context.h"
#include "scaler/io/ymq/io_socket.h"
#include "scaler/io/ymq/simple_interface.h"

using namespace scaler::ymq;

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
        using namespace std::chrono_literals;
        std::this_thread::sleep_for(10s);
    }

    return 0;
}

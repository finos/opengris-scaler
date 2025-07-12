

#include <stdio.h>
#include <unistd.h>

#include <future>
#include <memory>

#include "scaler/io/ymq/io_context.h"
#include "scaler/io/ymq/io_socket.h"

int main() {
    IOContext context;

    auto createSocketPromise         = std::promise<void>();
    auto createSocketFuture          = createSocketPromise.get_future();
    std::shared_ptr<IOSocket> socket = context.createIOSocket(
        "ServerSocket", IOSocketType::Multicast, [&createSocketPromise] { createSocketPromise.set_value(); });
    createSocketFuture.wait();
    printf("Successfully created socket.\n");

    auto bind_promise = std::promise<void>();
    auto bind_future  = bind_promise.get_future();
    // Optionally handle bind result
    socket->bindTo("tcp://127.0.0.1:8080", [&bind_promise](int result) { bind_promise.set_value(); });
    bind_future.wait();
    printf("Successfully bound socket\n");

    while (true) {
        std::string address("");
        std::string payload("Hello from the publisher\n");

        Message publishContent;
        publishContent.address = Bytes(address.data(), address.size());
        publishContent.payload = Bytes(payload.data(), payload.size());

        auto send_promise = std::promise<void>();
        auto send_future  = send_promise.get_future();
        socket->sendMessage(std::move(publishContent), [&send_promise](int) { send_promise.set_value(); });
        send_future.wait();

        printf("One message published, sleep for 10 sec\n");
        using namespace std::chrono_literals;
        std::this_thread::sleep_for(10s);
    }

    return 0;
}

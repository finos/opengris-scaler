

#include <stdio.h>
#include <unistd.h>

#include <future>
#include <memory>

#include "scaler/io/ymq/io_context.h"
#include "scaler/io/ymq/io_socket.h"

int main() {
    auto createSocketPromise = std::make_shared<std::promise<void>>();
    auto createSocketFuture  = createSocketPromise->get_future();

    IOContext context;
    std::shared_ptr<IOSocket> socket = context.createIOSocket(
        "ServerSocket", IOSocketType::Dealer, [createSocketPromise] { createSocketPromise->set_value(); });

    createSocketFuture.wait();
    printf("Successfully created socket.\n");
    // using namespace std::chrono_literals;
    // std::this_thread::sleep_for(100ms);

    auto bind_promise = std::make_shared<std::promise<void>>();
    auto bind_future  = bind_promise->get_future();

    socket->bindTo("tcp://127.0.0.1:8080", [bind_promise](int result) {
        // Optionally handle result
        bind_promise->set_value();
    });

    printf("Waiting for bind to complete...\n");
    bind_future.wait();
    printf("Successfully bound socket\n");

    while (true) {
        printf("Try to recv a message\n");

        auto recv_promise = std::make_shared<std::promise<Message>>();
        auto recv_future  = recv_promise->get_future();

        socket->recvMessage([socket, recv_promise](Message msg) { recv_promise->set_value(std::move(msg)); });

        Message received_msg = recv_future.get();
        printf(
            "Receiving message from '%s', message content is: '%s'\n",
            received_msg.address.as_string().c_str(),
            std::string(received_msg.payload.data(), received_msg.payload.data() + received_msg.payload.len()).c_str());

        auto send_promise = std::make_shared<std::promise<void>>();
        auto send_future  = send_promise->get_future();

        socket->sendMessage(received_msg, [send_promise](int) { send_promise->set_value(); });

        send_future.wait();
        printf("Message echoed back. Looping...\n");
    }

    return 0;
}

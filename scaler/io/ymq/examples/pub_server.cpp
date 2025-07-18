

#include <stdio.h>
#include <unistd.h>

#include <future>
#include <memory>

#include "./common.h"
#include "scaler/io/ymq/io_context.h"
#include "scaler/io/ymq/io_socket.h"

using namespace scaler::ymq;

int main() {
    IOContext context;

    auto socket = syncCreateSocket(context, IOSocketType::Multicast, "ServerSocket");
    printf("Successfully created socket.\n");

    syncBindSocket(socket, "tcp://127.0.0.1:8080");
    printf("Successfully bound socket\n");

    while (true) {
        std::string address("");
        std::string payload("Hello from the publisher");

        Message publishContent;
        publishContent.address = Bytes::copy((uint8_t*)address.data(), address.size());
        publishContent.payload = Bytes::copy((uint8_t*)payload.data(), payload.size());

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



// C++
#include <stdio.h>
#include <unistd.h>

#include <chrono>
#include <future>
#include <memory>
#include <print>
#include <string>
#include <thread>

#include "./common.h"
#include "scaler/io/ymq/io_context.h"
#include "scaler/io/ymq/io_socket.h"
#include "scaler/io/ymq/typedefs.h"

using namespace scaler::ymq;
using namespace std::chrono;
using namespace std::chrono_literals;

int main(int argc, char* argv[]) {
    if (argc != 5) {
        printf("Usage: %s <Identity> <MessageSize> <MessageCount> <AddressString>\n", argv[0]);
        exit(1);
    }
    const std::string identity(argv[1]);
    const std::string longStr(std::stoi(argv[2]), '1');
    const size_t msgCnt = std::stoi(argv[3]);
    const std::string address(argv[4]);

    IOContext context;

    auto clientSocket = syncCreateSocket(context, IOSocketType::Connector, identity);
    printf("Successfully created socket.\n");

    syncConnectSocket(clientSocket, address);
    printf("Connected to server.\n");

    const std::string_view line = longStr;

    time_point<system_clock> start = system_clock::now();
    for (int cnt = 0; cnt < msgCnt; ++cnt) {
        Message message {};
        message.payload = Bytes {const_cast<char*>(line.data()), line.size()};

        std::promise<void> sendPromise;
        auto sendFuture = sendPromise.get_future();

        clientSocket->sendMessage(std::move(message), [&sendPromise](int) { sendPromise.set_value(); });
        sendFuture.get();

        std::promise<Message> recvPromise;
        auto recvFuture = recvPromise.get_future();
        clientSocket->recvMessage([&recvPromise](Message msg) { recvPromise.set_value(std::move(msg)); });
        recvFuture.get();
    }

    time_point<system_clock> end = system_clock::now();

    printf("Send and recv %lu messages with %lu bytes.\n", msgCnt, longStr.size());
    auto milli = duration_cast<milliseconds>(end - start);
    std::print("Spend {}\n", milli);
    std::print("Throughput {} Bpms\n", msgCnt * (longStr.size()) * 1.0 / milli.count());

    context.removeIOSocket(clientSocket);

    return 0;
}



// C++
#include <stdio.h>
#include <unistd.h>

#include <future>
#include <memory>
#include <string>

#include "./common.h"
#include "scaler/io/ymq/io_context.h"
#include "scaler/io/ymq/io_socket.h"
#include "scaler/io/ymq/typedefs.h"

std::string longStr = "1234567890";

using namespace scaler::ymq;
using namespace std::chrono_literals;

int main() {
    IOContext context;

    for (int i = 0; i < 400; ++i)
        longStr += "1234567890";

    auto clientSocket = syncCreateSocket(context, IOSocketType::Connector, "ClientSocket");
    printf("Successfully created socket.\n");

    constexpr size_t msgCnt = 100'000;

    std::vector<std::promise<void>> sendPromises;
    sendPromises.reserve(msgCnt + 10);
    std::vector<std::promise<Message>> recvPromises;
    recvPromises.reserve(msgCnt + 10);

    syncConnectSocket(clientSocket, "tcp://127.0.0.1:8080");
    printf("Connected to server.\n");

    const std::string_view line = longStr;

    for (int cnt = 0; cnt < msgCnt; ++cnt) {
        Message message;
        std::string destAddress = "ServerSocket";

        message.address = Bytes {const_cast<char*>(destAddress.c_str()), destAddress.size()};
        message.payload = Bytes {const_cast<char*>(line.data()), line.size()};

        sendPromises.emplace_back();

        clientSocket->sendMessage(
            std::move(message), [&send_promise = sendPromises.back()](int) { send_promise.set_value(); });

        recvPromises.emplace_back();

        clientSocket->recvMessage(
            [&recv_promise = recvPromises.back()](Message msg) { recv_promise.set_value(std::move(msg)); });
    }

    for (auto& x: sendPromises) {
        auto future = x.get_future();
        future.get();
    }
    printf("send completes\n");

    for (auto& x: recvPromises) {
        auto future = x.get_future();
        Message msg = future.get();
    }
    printf("recv completes\n");

    printf("Send and recv %lu messages, checksum fits, exiting.\n", msgCnt);

    context.removeIOSocket(clientSocket);

    return 0;
}

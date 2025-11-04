
#include <future>
#include <iostream>
#include <memory>
#include <string>

#include "scaler/ymq/io_context.h"
#include "scaler/ymq/io_socket.h"
#include "scaler/ymq/simple_interface.h"
#include "scaler/ymq/typedefs.h"

std::string longStr = "1234567890";

using namespace scaler::ymq;
using namespace std::chrono_literals;

int main()
{
    IOContext context;

    for (int i = 0; i < 400; ++i)
        longStr += "1234567890";

    auto clientSocket = syncCreateSocket(context, IOSocketType::Connector, "ClientSocket");
    std::cout << "Successfully created socket.\n";

    constexpr size_t msgCnt = 100'000;

    std::vector<std::promise<std::expected<void, Error>>> sendPromises;
    sendPromises.reserve(msgCnt + 10);
    std::vector<std::promise<std::pair<Message, Error>>> recvPromises;
    recvPromises.reserve(msgCnt + 10);

    syncConnectSocket(clientSocket, "tcp://127.0.0.1:8080");
    std::cout << "Connected to server.\n";

    const std::string_view line = longStr;

    for (size_t cnt = 0; cnt < msgCnt; ++cnt) {
        Message message;
        std::string destAddress = "ServerSocket";

        message.address = Bytes {const_cast<char*>(destAddress.c_str()), destAddress.size()};
        message.payload = Bytes {const_cast<char*>(line.data()), line.size()};

        sendPromises.emplace_back();

        clientSocket->sendMessage(
            std::move(message),
            [&send_promise = sendPromises.back()](std::expected<void, Error>) { send_promise.set_value({}); });

        recvPromises.emplace_back();

        clientSocket->recvMessage([&recv_promise = recvPromises.back()](std::pair<Message, Error> msg) {
            recv_promise.set_value(std::move(msg));
        });
    }

    for (auto& x: sendPromises) {
        auto future = x.get_future();
        future.wait();
    }
    std::cout << "Send completes.\n";

    for (auto&& x: recvPromises) {
        auto future = x.get_future();
        Message msg = future.get().first;
        if (msg.payload.as_string() != longStr) {
            std::cerr << "Checksum failed, " << *msg.payload.as_string() << std::endl;
            exit(1);
        }
    }
    std::cout << "Recv completes.\n";

    std::cout << "Send and recv " << msgCnt << " messages, checksum fits, exiting.\n";

    context.removeIOSocket(clientSocket);

    return 0;
}

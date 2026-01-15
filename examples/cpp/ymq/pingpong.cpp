#include <chrono>
#include <future>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include "scaler/error/error.h"
#include "scaler/ymq/io_context.h"
#include "scaler/ymq/io_socket.h"
#include "scaler/ymq/simple_interface.h"
#include "scaler/ymq/typedefs.h"

using scaler::ymq::Bytes;
using scaler::ymq::Error;
using scaler::ymq::IOContext;
using scaler::ymq::IOSocketType;
using scaler::ymq::Message;
using scaler::ymq::syncConnectSocket;
using scaler::ymq::syncCreateSocket;

int main(int argc, char* argv[])
{
    if (argc != 5) {
        std::cout << "Usage: " << argv[0] << " <Identity> <MessageSize> <MessageCount> <AddressString>\n";
        exit(1);
    }
    const std::string identity(argv[1]);
    const std::string longStr(std::stoi(argv[2]), '1');
    const size_t msgCnt = std::stoi(argv[3]);
    const std::string address(argv[4]);

    IOContext context;

    auto clientSocket = syncCreateSocket(context, IOSocketType::Connector, identity);
    std::cout << "Successfully created socket.\n";

    syncConnectSocket(clientSocket, address);
    std::cout << "Connected to server.\n";

    const std::string_view line = longStr;

    std::chrono::time_point<std::chrono::system_clock> start = std::chrono::system_clock::now();
    for (size_t cnt = 0; cnt < msgCnt; ++cnt) {
        Message message {};
        message.payload = Bytes {const_cast<char*>(line.data()), line.size()};

        std::promise<std::expected<void, Error>> sendPromise;
        auto sendFuture = sendPromise.get_future();

        clientSocket->sendMessage(
            std::move(message), [&sendPromise](std::expected<void, Error>) { sendPromise.set_value({}); });
        sendFuture.get().value();

        std::promise<std::pair<Message, Error>> recvPromise;
        auto recvFuture = recvPromise.get_future();
        clientSocket->recvMessage(
            [&recvPromise](std::pair<Message, Error> msg) { recvPromise.set_value(std::move(msg)); });
        recvFuture.wait();
    }

    std::chrono::time_point<std::chrono::system_clock> end = std::chrono::system_clock::now();

    std::cout << "Send and recv " << msgCnt << " messages with " << longStr.size() << " bytes.\n";
    auto milli = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "Spend " << milli.count() << "ms.\n";
    std::cout << "Throughput " << msgCnt * (longStr.size()) * 1.0 / milli.count() << " Bpms.\n";

    context.removeIOSocket(clientSocket);

    return 0;
}

#include <chrono>
#include <iostream>
#include <string>

#include "scaler/ymq/io_context.h"
#include "scaler/ymq/sync/connector_socket.h"

using scaler::ymq::Bytes;
using scaler::ymq::IOContext;
using scaler::ymq::sync::ConnectorSocket;

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

    auto result = ConnectorSocket::connect(context, identity, address);
    if (!result.has_value()) {
        std::cerr << "Failed to connect: " << result.error().what() << std::endl;
        return 1;
    }

    auto socket = std::move(result.value());
    std::cout << "Connected to server.\n";

    std::chrono::time_point<std::chrono::system_clock> start = std::chrono::system_clock::now();
    for (size_t cnt = 0; cnt < msgCnt; ++cnt) {
        auto sendResult = socket.sendMessage(Bytes {longStr});
        if (!sendResult.has_value()) {
            std::cerr << "Failed to send message: " << sendResult.error().what() << std::endl;
            return 1;
        }

        auto recvResult = socket.recvMessage();
        if (!recvResult.has_value()) {
            std::cerr << "Failed to receive message: " << recvResult.error().what() << std::endl;
            return 1;
        }
    }

    std::chrono::time_point<std::chrono::system_clock> end = std::chrono::system_clock::now();

    std::cout << "Send and recv " << msgCnt << " messages with " << longStr.size() << " bytes.\n";
    auto milli = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "Spend " << milli.count() << "ms.\n";
    std::cout << "Throughput " << msgCnt * (longStr.size()) * 1.0 / milli.count() << " Bpms.\n";

    return 0;
}

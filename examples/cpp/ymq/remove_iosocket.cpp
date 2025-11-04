#include <iostream>
#include <memory>
#include <string>

#include "scaler/ymq/io_context.h"
#include "scaler/ymq/simple_interface.h"
#include "scaler/ymq/typedefs.h"

using namespace scaler::ymq;

int main()
{
    IOContext context;
    auto clientSocket = syncCreateSocket(context, IOSocketType::Connector, "ServerSocket");
    std::cout << "Successfully created socket.\n";

    syncConnectSocket(clientSocket, "tcp://127.0.0.1:8080");
    std::cout << "Connected to server.\n";

    context.removeIOSocket(clientSocket);

    return 0;
}

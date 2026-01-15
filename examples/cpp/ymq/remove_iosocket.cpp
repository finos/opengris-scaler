#include <iostream>
#include <memory>
#include <string>

#include "scaler/ymq/io_context.h"
#include "scaler/ymq/simple_interface.h"
#include "scaler/ymq/typedefs.h"

using scaler::ymq::IOContext;
using scaler::ymq::IOSocketType;
using scaler::ymq::syncConnectSocket;
using scaler::ymq::syncCreateSocket;

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

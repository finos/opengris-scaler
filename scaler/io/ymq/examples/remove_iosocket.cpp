#include <stdio.h>

#include <memory>
#include <string>

#include "scaler/io/ymq/io_context.h"
#include "scaler/io/ymq/simple_interface.h"
#include "scaler/io/ymq/typedefs.h"

using namespace scaler::ymq;

int main()
{
    IOContext context;
    auto clientSocket = syncCreateSocket(context, IOSocketType::Connector, "ServerSocket");
    printf("Successfully created socket.\n");

    syncConnectSocket(clientSocket, "tcp://127.0.0.1:8080");
    printf("Connected to server.\n");

    context.removeIOSocket(clientSocket);

    return 0;
}

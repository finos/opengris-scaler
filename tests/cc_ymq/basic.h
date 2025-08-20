#pragma once

#include "tests/cc_ymq/common.h"
#include "scaler/io/ymq/examples/common.h"
#include "scaler/io/ymq/io_context.h"

TestResult basic_server_main()
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Binder, "server");
    syncBindSocket(socket, "tcp://127.0.0.1:25711");
    auto result = syncRecvMessage(socket);

    ASSERT(result.has_value());
    ASSERT(result->payload.as_string() == "yi er san si wu liu");

    context.removeIOSocket(socket);

    return TestResult::Success;
}

TestResult basic_client_main(int delay)
{
    TcpSocket socket;

    socket.connect("127.0.0.1", 25711);

    socket.write_message("client");
    auto remote_identity = socket.read_message();
    ASSERT(remote_identity == "server");
    socket.write_message("yi er san si wu liu");

    if (delay)
        std::this_thread::sleep_for(std::chrono::seconds(delay));

    return TestResult::Success;
}

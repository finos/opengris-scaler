#pragma once

#include "tests/cc_ymq/common.h"
#include "scaler/io/ymq/examples/common.h"
#include "scaler/io/ymq/io_context.h"

TestResult big_message_server_main()
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Binder, "server");
    syncBindSocket(socket, "tcp://127.0.0.1:25711");
    auto result = syncRecvMessage(socket);

    ASSERT(result.has_value());
    ASSERT(result->payload.len() == 500'000'000);

    context.removeIOSocket(socket);

    return TestResult::Success;
}

TestResult big_message_client_main(int delay)
{
    TcpSocket socket;

    socket.connect("127.0.0.1", 25711);

    socket.write_message("client");
    auto remote_identity = socket.read_message();
    ASSERT(remote_identity == "server");
    std::string msg(500'000'000, '.');
    socket.write_message(msg);

    if (delay)
        std::this_thread::sleep_for(std::chrono::seconds(delay));

    return TestResult::Success;
}

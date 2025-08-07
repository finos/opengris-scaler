#pragma once

#include <thread>

#include "scaler/io/ymq/examples/common.h"
#include "scaler/io/ymq/io_context.h"
#include "tests/cc_ymq/common.h"

TestResult slow_server_main()
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Binder, "server");
    syncBindSocket(socket, "tcp://127.0.0.1:25713");
    auto result = syncRecvMessage(socket);

    ASSERT(result.has_value());
    ASSERT(result->payload.as_string() == "yi er san si wu liu");

    context.removeIOSocket(socket);

    return TestResult::Success;
}

// TODO: implement this using mitm
TestResult slow_client_main()
{
    TcpSocket socket;

    socket.connect("127.0.0.1", 25713);

    socket.write_message("client");
    auto remote_identity = socket.read_message();
    ASSERT(remote_identity == "server");

    std::string message = "yi er san si wu liu";
    uint64_t header     = message.length();

    socket.write_all((char*)&header, 4);
    std::this_thread::sleep_for(5s);
    socket.write_all((char*)&header + 4, 4);
    std::this_thread::sleep_for(3s);
    socket.write_all(message.data(), header / 2);
    std::this_thread::sleep_for(5s);
    socket.write_all(message.data() + header / 2, header - header / 2);
    std::this_thread::sleep_for(3s);

    return TestResult::Success;
}

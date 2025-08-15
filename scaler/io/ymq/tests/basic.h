#pragma once

#include "scaler/io/ymq/tests/common.h"
#include "scaler/io/ymq/examples/common.h"
#include "scaler/io/ymq/io_context.h"

void basic_server_main()
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Binder, "server");
    syncBindSocket(socket, "tcp://127.0.0.1:25711");
    auto result = syncRecvMessage(socket);

    assert(result.has_value());
    assert(result->payload.as_string() == "yi er san si wu liu");

    context.removeIOSocket(socket);
}

void basic_client_main(int delay)
{
    TcpSocket socket;

    socket.connect("127.0.0.1", 25711);

    socket.write_message("client");
    auto remote_identity = socket.read_message();
    assert(remote_identity == "server");
    socket.write_message("yi er san si wu liu");

    if (delay)
        std::this_thread::sleep_for(std::chrono::seconds(delay));
}

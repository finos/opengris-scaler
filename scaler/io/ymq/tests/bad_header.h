#pragma once

#include <limits>
#include <thread>
#include "scaler/io/ymq/examples/common.h"
#include "scaler/io/ymq/io_context.h"
#include "scaler/io/ymq/tests/common.h"

void bad_header_server_main()
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Binder, "server");
    syncBindSocket(socket, "tcp://127.0.0.1:25713");
    auto result = syncRecvMessage(socket);

    assert(result.has_value());
    assert(result->payload.as_string() == "yi er san si wu liu");

    context.removeIOSocket(socket);
}

void bad_header_client_main()
{
    TcpSocket socket;

    socket.connect("127.0.0.1", 25713);

    socket.write_message("client");
    auto remote_identity = socket.read_message();
    assert(remote_identity == "server");

    uint64_t header = std::numeric_limits<uint64_t>::max();
    socket.write_all((char*)&header, 8);

    std::this_thread::sleep_for(3s);
}

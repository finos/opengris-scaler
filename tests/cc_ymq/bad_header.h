#pragma once

#include <limits>
#include <thread>

#include "scaler/io/ymq/examples/common.h"
#include "scaler/io/ymq/io_context.h"
#include "tests/cc_ymq/common.h"

TestResult bad_header_server_main()
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

TestResult bad_header_client_main()
{
    TcpSocket socket;

    socket.connect("127.0.0.1", 25713);

    socket.write_message("client");
    auto remote_identity = socket.read_message();
    ASSERT(remote_identity == "server");

    uint64_t header = std::numeric_limits<uint64_t>::max();
    socket.write_all((char*)&header, 8);

    // TODO: this sleep shouldn't be necessary
    std::this_thread::sleep_for(3s);

    return TestResult::Success;
}

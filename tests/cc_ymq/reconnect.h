#pragma once

#include <cstdio>
#include <exception>
#include <string>


#include "scaler/io/ymq/bytes.h"
#include "tests/cc_ymq/common.h"
#include "scaler/io/ymq/examples/common.h"
#include "scaler/io/ymq/io_context.h"

TestResult reconnect_server_main()
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Binder, "server");
    syncBindSocket(socket, "tcp://192.0.2.1:23571");
    auto result = syncRecvMessage(socket);

    ASSERT(result.has_value());
    ASSERT(result->payload.as_string() == "hello!!");

    context.removeIOSocket(socket);

    return TestResult::Success;
}

TestResult reconnect_client_main(int delay)
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Connector, "client");
    syncConnectSocket(socket, "tcp://192.0.2.2:2323");
    auto result = syncSendMessage(socket, {
        .address = Bytes("server"),
        .payload = Bytes("hello!!")
    });

    context.removeIOSocket(socket);

    return TestResult::Success;
}


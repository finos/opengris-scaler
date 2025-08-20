#pragma once

#include <limits>

#include "scaler/io/ymq/bytes.h"
#include "scaler/io/ymq/examples/common.h"
#include "scaler/io/ymq/io_context.h"
#include "tests/cc_ymq/common.h"

TestResult empty_message_server_main()
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Binder, "server");
    syncBindSocket(socket, "tcp://127.0.0.1:25713");

    auto result = syncRecvMessage(socket);
    ASSERT(result.has_value());
    ASSERT(result->payload.as_string() == "");

    auto result2 = syncRecvMessage(socket);
    ASSERT(result2.has_value());
    ASSERT(result2->payload.as_string() == "");

    context.removeIOSocket(socket);

    return TestResult::Success;
}

TestResult empty_message_client_main()
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Connector, "client");
    syncConnectSocket(socket, "tcp://127.0.0.1:25713");

    auto error = syncSendMessage(socket, Message {.address = Bytes(), .payload = Bytes()});
    ASSERT(!error);

    auto error2 = syncSendMessage(socket, Message {.address = Bytes(), .payload = Bytes("")});
    ASSERT(!error2);

    context.removeIOSocket(socket);

    return TestResult::Success;
}

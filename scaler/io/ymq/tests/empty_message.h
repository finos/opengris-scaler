#pragma once

#include <limits>
#include <print>
#include <thread>

#include "scaler/io/ymq/bytes.h"
#include "scaler/io/ymq/examples/common.h"
#include "scaler/io/ymq/io_context.h"
#include "scaler/io/ymq/tests/common.h"

void empty_message_server_main()
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Binder, "server");
    syncBindSocket(socket, "tcp://127.0.0.1:25713");

    auto result = syncRecvMessage(socket);
    assert(result.has_value());

    // which of these is the desired behaviour?
    // assert(result->payload.is_empty());
    assert(result->payload.as_string() == "");

    auto result2 = syncRecvMessage(socket);
    assert(result2.has_value());
    assert(result2->payload.as_string() == "");

    context.removeIOSocket(socket);
}

void empty_message_client_main()
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Connector, "client");
    syncConnectSocket(socket, "tcp://127.0.0.1:25713");

    auto error = syncSendMessage(socket, Message {.address = Bytes(), .payload = Bytes()});
    assert(!error);

    auto error2 = syncSendMessage(socket, Message {.address = Bytes(), .payload = Bytes("")});
    assert(!error2);

    context.removeIOSocket(socket);
}

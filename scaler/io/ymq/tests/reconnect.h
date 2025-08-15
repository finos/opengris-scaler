#pragma once

#include <memory>
#include <print>
#include <semaphore>
#include <thread>

#include "scaler/io/ymq/bytes.h"
#include "scaler/io/ymq/examples/common.h"
#include "scaler/io/ymq/io_context.h"
#include "scaler/io/ymq/tests/common.h"

struct ReconnectContext {
    std::shared_ptr<std::binary_semaphore> sem_a;
    std::shared_ptr<std::binary_semaphore> sem_b;

    ReconnectContext()
        : sem_a(std::make_shared<std::binary_semaphore>(0)), sem_b(std::make_shared<std::binary_semaphore>(0))
    {
    }
};

void reconnect_server_main(ReconnectContext ctx)
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Binder, "server");
    syncBindSocket(socket, "tcp://127.0.0.1:25714");

    auto result = syncRecvMessage(socket);
    std::println("+A");
    assert(result.has_value() && result->address.as_string() == "client" && result->payload.as_string() == "ready");
    std::println("+B");
    ctx.sem_a->release();
    std::println("+C");
    ctx.sem_b->acquire();
    std::println("+D");
    auto result2 = syncSendMessage(socket, Message {.address = Bytes("client"), .payload = Bytes("reconnect?")});
    std::println("+E");
    std::this_thread::sleep_for(1s);
    ctx.sem_a->release();
    std::println("+F");

    context.removeIOSocket(socket);
}

void reconnect_client_main(ReconnectContext ctx)
{
    {
        TcpSocket socket;

        socket.connect("127.0.0.1", 25714);

        socket.write_message("client");
        auto remote_identity = socket.read_message();
        assert(remote_identity == "server");
        socket.write_message("ready");
        ctx.sem_a->acquire();
    }

    ctx.sem_b->release();
    ctx.sem_a->acquire();

    {
        TcpSocket socket;

        socket.connect("127.0.0.1", 25714);

        socket.write_message("client");
        auto remote_identity = socket.read_message();
        assert(remote_identity == "server");
        auto message = socket.read_message();
        assert(message == "reconnect?");
    }
}

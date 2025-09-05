#pragma once

#include <print>
#include <thread>

#include "scaler/io/ymq/examples/common.h"
#include "scaler/io/ymq/io_context.h"
#include "tests/cc_ymq/common.h"

void incomplete_identity_server_main()
{
    IOContext context(1); 

    auto socket = syncCreateSocket(context, IOSocketType::Binder, "server");
    syncBindSocket(socket, "tcp://127.0.0.1:25715");
    auto result = syncRecvMessage(socket);

    assert(result.has_value());
    assert(result->payload.as_string() == "yi er san si wu liu");

    context.removeIOSocket(socket);
}

void incomplete_identity_client_main()
{
    // open a socket, write an incomplete identity and exit
    {
        TcpSocket socket;

        socket.connect("127.0.0.1", 25715);

        auto remote_identity = socket.read_message();
        assert(remote_identity == "server");

        // write incomplete identity and exit
        std::string identity = "client";
        uint64_t header      = identity.length();
        socket.write_all((char*)&header, 8);
        socket.write_all(identity.data(), identity.length() - 2);
        std::this_thread::sleep_for(3s);
    }

    // connect again and try to send a message
    {
        TcpSocket socket;
        socket.connect("127.0.0.1", 25715);
        auto remote_identity = socket.read_message();
        assert(remote_identity == "server");
        socket.write_message("client");
        socket.write_message("yi er san si wu liu");
        std::this_thread::sleep_for(3s);
    }
}

#pragma once

#include <unistd.h>

#include <future>
#include <memory>

#include "scaler/io/ymq/error.h"
#include "scaler/io/ymq/io_context.h"
#include "scaler/io/ymq/io_socket.h"

// We should not be using namespace in header file, but this is example, so we are good
using namespace scaler::ymq;

inline std::shared_ptr<IOSocket> syncCreateSocket(IOContext& context, IOSocketType type, std::string name)
{
    auto createSocketPromise = std::promise<std::shared_ptr<IOSocket>>();
    auto createSocketFuture  = createSocketPromise.get_future();
    context.createIOSocket(
        std::move(name), type, [&createSocketPromise](auto sock) { createSocketPromise.set_value(sock); });

    auto clientSocket = createSocketFuture.get();
    return clientSocket;
}

inline void syncBindSocket(std::shared_ptr<IOSocket> socket, std::string address)
{
    auto bind_promise = std::promise<std::expected<void, Error>>();
    auto bind_future  = bind_promise.get_future();
    // Optionally handle result in the callback
    socket->bindTo(address, [&bind_promise](std::expected<void, Error> result) { bind_promise.set_value({}); });
    bind_future.wait();
}

inline void syncConnectSocket(std::shared_ptr<IOSocket> socket, std::string address)
{
    auto connect_promise = std::promise<std::expected<void, Error>>();
    auto connect_future  = connect_promise.get_future();

    socket->connectTo(
        address, [&connect_promise](std::expected<void, Error> result) { connect_promise.set_value({}); });

    connect_future.wait();
}

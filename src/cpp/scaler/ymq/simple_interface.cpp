
#include "scaler/ymq/simple_interface.h"

#include <optional>

namespace scaler {
namespace ymq {

std::shared_ptr<IOSocket> syncCreateSocket(IOContext& context, IOSocketType type, std::string name)
{
    auto createSocketPromise = std::promise<std::shared_ptr<IOSocket>>();
    auto createSocketFuture  = createSocketPromise.get_future();
    context.createIOSocket(
        std::move(name), type, [&createSocketPromise](auto sock) { createSocketPromise.set_value(sock); });

    auto clientSocket = createSocketFuture.get();
    return clientSocket;
}

void syncBindSocket(std::shared_ptr<IOSocket> socket, std::string address)
{
    auto bind_promise = std::promise<std::expected<void, Error>>();
    auto bind_future  = bind_promise.get_future();
    // Optionally handle result in the callback
    socket->bindTo(address, [&bind_promise](std::expected<void, Error> result) { bind_promise.set_value(result); });
    bind_future.wait();
}

void syncConnectSocket(std::shared_ptr<IOSocket> socket, std::string address)
{
    auto connect_promise = std::promise<std::expected<void, Error>>();
    auto connect_future  = connect_promise.get_future();

    socket->connectTo(
        address, [&connect_promise](std::expected<void, Error> result) { connect_promise.set_value(result); });

    connect_future.wait();
}

std::expected<Message, Error> syncRecvMessage(std::shared_ptr<IOSocket> socket)
{
    auto fut = futureRecvMessage(std::move(socket));
    return fut.get();
}

std::optional<Error> syncSendMessage(std::shared_ptr<IOSocket> socket, Message message)
{
    auto fut = futureSendMessage(std::move(socket), std::move(message));
    return fut.get();
}

std::future<std::expected<Message, Error>> futureRecvMessage(std::shared_ptr<IOSocket> socket)
{
    auto recv_promise_ptr = std::make_unique<std::promise<std::expected<Message, Error>>>();
    auto recv_future      = recv_promise_ptr->get_future();
    socket->recvMessage([recv_promise = std::move(recv_promise_ptr)](std::pair<Message, Error> result) {
        if (result.second._errorCode == Error::ErrorCode::Uninit)
            recv_promise->set_value(std::move(result.first));
        else
            recv_promise->set_value(std::unexpected {std::move(result.second)});
    });
    return recv_future;
}

std::future<std::optional<Error>> futureSendMessage(std::shared_ptr<IOSocket> socket, Message message)
{
    auto send_promise_ptr = std::make_unique<std::promise<std::optional<Error>>>();
    auto send_future      = send_promise_ptr->get_future();
    socket->sendMessage(
        std::move(message), [send_promise = std::move(send_promise_ptr)](std::expected<void, Error> result) {
            if (result)
                send_promise->set_value(std::nullopt);
            else
                send_promise->set_value(std::move(result.error()));
        });
    return send_future;
}

}  // namespace ymq
}  // namespace scaler

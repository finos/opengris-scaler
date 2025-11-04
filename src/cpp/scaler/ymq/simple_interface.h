#pragma once

#include <future>
#include <memory>

#include "scaler/error/error.h"
#include "scaler/ymq/io_context.h"
#include "scaler/ymq/io_socket.h"

namespace scaler {
namespace ymq {

std::shared_ptr<IOSocket> syncCreateSocket(IOContext& context, IOSocketType type, std::string name);
void syncBindSocket(std::shared_ptr<IOSocket> socket, std::string address);
void syncConnectSocket(std::shared_ptr<IOSocket> socket, std::string address);

std::expected<Message, Error> syncRecvMessage(std::shared_ptr<IOSocket> socket);
std::optional<Error> syncSendMessage(std::shared_ptr<IOSocket> socket, Message message);

std::future<std::expected<Message, Error>> futureRecvMessage(std::shared_ptr<IOSocket> socket);
std::future<std::optional<Error>> futureSendMessage(std::shared_ptr<IOSocket> socket, Message message);

}  // namespace ymq
}  // namespace scaler

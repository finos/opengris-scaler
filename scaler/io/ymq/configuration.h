#pragma once

// C++
#include <expected>
#include <functional>
#include <memory>
#include <string>

// Because the devil says "You shall live with errors".
// ^-- The linker complains when the file is not here.
#include "scaler/io/ymq/error.h"

// Because the devil casts spells in plain English.
#ifdef _WIN32
#undef SendMessageCallback
#define __PRETTY_FUNCTION__ __FUNCSIG__
#endif  // _WIN32

namespace scaler {
namespace ymq {

class EpollContext;
class IocpContext;
class Message;
class IOSocket;

constexpr const uint64_t IOCP_SOCKET_CLOSED = 4;

struct Configuration {
#ifdef __linux__
    using PollingContext = EpollContext;
#endif  // __linux__
#ifdef _WIN32
    using PollingContext = IocpContext;
#endif  // _WIN32

    using IOSocketIdentity                = std::string;
    using SendMessageCallback             = std::move_only_function<void(std::expected<void, Error>)>;
    using RecvMessageCallback             = std::move_only_function<void(std::pair<Message, Error>)>;
    using ConnectReturnCallback           = std::move_only_function<void(std::expected<void, Error>)>;
    using BindReturnCallback              = std::move_only_function<void(std::expected<void, Error>)>;
    using CreateIOSocketCallback          = std::move_only_function<void(std::shared_ptr<IOSocket>)>;
    using TimedQueueCallback              = std::move_only_function<void()>;
    using ExecutionFunction               = std::move_only_function<void()>;
    using ExecutionCancellationIdentifier = size_t;
};

}  // namespace ymq
}  // namespace scaler

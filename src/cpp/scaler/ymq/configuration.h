#pragma once

// C++
#include <expected>
#include <functional>
#include <memory>
#include <string>

// Because the devil says "You shall live with errors".
// ^-- The linker complains when the file is not here.
#include "scaler/error/error.h"
#include "scaler/utility/move_only_function.h"

namespace scaler {
namespace ymq {

class EpollContext;
class IOCPContext;
struct Message;
class IOSocket;

constexpr const uint64_t IOCP_SOCKET_CLOSED = 4;

struct Configuration {
#ifdef __linux__
    using PollingContext = EpollContext;
#endif  // __linux__
#ifdef _WIN32
    using PollingContext = IOCPContext;
#endif  // _WIN32

    using IOSocketIdentity                = std::string;
    using SendMessageCallback             = utility::MoveOnlyFunction<void(std::expected<void, Error>)>;
    using RecvMessageCallback             = utility::MoveOnlyFunction<void(std::pair<Message, Error>)>;
    using ConnectReturnCallback           = utility::MoveOnlyFunction<void(std::expected<void, Error>)>;
    using BindReturnCallback              = utility::MoveOnlyFunction<void(std::expected<void, Error>)>;
    using CreateIOSocketCallback          = utility::MoveOnlyFunction<void(std::shared_ptr<IOSocket>)>;
    using TimedQueueCallback              = utility::MoveOnlyFunction<void()>;
    using ExecutionFunction               = utility::MoveOnlyFunction<void()>;
    using ExecutionCancellationIdentifier = size_t;
};

}  // namespace ymq
}  // namespace scaler

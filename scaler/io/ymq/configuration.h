#pragma once

// C++
#include <functional>
#include <string>

class EpollContext;
class Message;

struct Configuration {
    using PollingContext                  = EpollContext;
    using IOSocketIdentity                = std::string;
    using SendMessageCallback             = std::move_only_function<void(int)>;
    using RecvMessageCallback             = std::move_only_function<void(Message)>;
    using ConnectReturnCallback           = std::move_only_function<void(int)>;
    using BindReturnCallback              = std::move_only_function<void(int)>;
    using CreateIOSocketCallback          = std::move_only_function<void()>;
    using TimedQueueCallback              = std::move_only_function<void()>;
    using ExecutionFunction               = std::move_only_function<void()>;
    using ExecutionCancellationIdentifier = size_t;
};

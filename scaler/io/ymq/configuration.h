#pragma once

// C++
#include <functional>
#include <string>

class EpollContext;
class Message;

struct Configuration {
    using PollingContext                  = EpollContext;
    using IOSocketIdentity                = std::string;
    using SendMessageCallback             = std::function<void(int)>;
    using RecvMessageCallback             = std::function<void(Message)>;
    using ConnectReturnCallback           = std::function<void(int)>;
    using BindReturnCallback              = std::function<void(int)>;
    using TimedQueueCallback              = std::function<void()>;
    using ExecutionCancellationIdentifier = size_t;
};

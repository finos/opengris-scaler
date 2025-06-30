#pragma once

// C++
#include <functional>
#include <memory>
#include <vector>

// First-party
#include "scaler/io/ymq/configuration.h"
#include "scaler/io/ymq/typedefs.h"

class IOSocket;
class EventLoopThread;

class IOContext {
    std::vector<std::shared_ptr<EventLoopThread>> _threads;

    using Identity = Configuration::IOSocketIdentity;

public:
    IOContext(size_t threadCount = 1);
    IOContext(const IOContext&)            = delete;
    IOContext& operator=(const IOContext&) = delete;
    IOContext(IOContext&&)                 = delete;
    IOContext& operator=(IOContext&&)      = delete;

    // These methods need to be thread-safe.
    std::shared_ptr<IOSocket> createIOSocket(
        Identity identity, IOSocketType socketType, std::function<void()> callback);

    // After user called this method, no other call on the passed in IOSocket should be made.
    void removeIOSocket(std::shared_ptr<IOSocket>& socket);

    size_t numThreads() const { return _threads.size(); }
};

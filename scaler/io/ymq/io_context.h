#pragma once

// C++
#include <memory>
#include <mutex>
#include <string>
#include <vector>

// First-party
#include "scaler/io/ymq/event_loop_thread.h"
#include "scaler/io/ymq/typedefs.h"

using Identity = Configuration::Identity;

class IOSocket;

class IOContext {
    // This is a pointer, just for now
    std::vector<std::shared_ptr<EventLoopThread>> _threads;

public:
    IOContext(size_t threadCount = 1);

    IOContext(const IOContext&)            = delete;
    IOContext& operator=(const IOContext&) = delete;
    IOContext(IOContext&&)                 = delete;
    IOContext& operator=(IOContext&&)      = delete;

    // These methods need to be thread-safe.
    std::shared_ptr<IOSocket> createIOSocket(Identity identity, IOSocketType socketType);
    bool removeIOSocket(std::shared_ptr<IOSocket>);

    size_t numThreads() const { return _threads.size(); }
};

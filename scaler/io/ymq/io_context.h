#pragma once

// C++
#include <memory>
#include <mutex>
#include <string>
#include <vector>

// First-party
#include "scaler/io/ymq/event_loop_thread.h"
#include "scaler/io/ymq/typedefs.h"

class IOSocket;

class IOContext {
    std::vector<std::shared_ptr<EventLoopThread>> _threads;
    std::mutex _threadsMu;

public:
    IOContext(size_t threadCount = 1);

    IOContext(const IOContext&)            = delete;
    IOContext& operator=(const IOContext&) = delete;
    IOContext(IOContext&&)                 = delete;
    IOContext& operator=(IOContext&&)      = delete;

    // These methods need to be thread-safe.
    IOSocket* createIOSocket(std::string identity, IOSocketType socketType);
    // ioSocket.getEventLoop().removeIOSocket(&ioSocket);
    bool removeIOSocket(IOSocket*);
};

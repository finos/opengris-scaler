#pragma once

// C++
#include <cstdint>  // uint64_t
#include <functional>
#include <memory>

// First-party
#include "scaler/io/ymq/event_loop_thread.h"

class EventLoopThread;

class EventManager {
    std::shared_ptr<EventLoopThread> eventLoop;
    // TODO: This may be FileDescriptor
    const int fd;
    // Implementation defined method, will call onRead, onWrite etc based on events

public:
    int events;
    int revents;
    void updateEvents();

    void onEvents(uint64_t events) {}
    // User that registered them should have everything they need
    // In the future, we might add more onXX() methods, for now these are all we need.
    using OnEventCallback = std::function<void()>;
    OnEventCallback onRead;
    OnEventCallback onWrite;
    OnEventCallback onClose;
    OnEventCallback onError;
    EventManager(): fd {} {}
};

#pragma once

// C++
#include <functional>

// First-party
#include "event_loop_thread.hpp"

class EventManager {
    EventLoopThread& eventLoop;
    const int fd;
    // Implementation defined method, will call onRead, onWrite etc based on events
    void onEvents();

public:
    int events;
    int revents;
    void updateEvents();

    // User that registered them should have everything they need
    // In the future, we might add more onXX() methods, for now these are all we need.
    using OnEventCallback = std::function<void()>;
    OnEventCallback onRead;
    OnEventCallback onWrite;
    OnEventCallback onClose;
    OnEventCallback onError;
};

#pragma once

// C++
#include <functional>

// First-party
#include "event_loop_backend.hpp"
#include "event_manager.hpp"
#include "interruptive_concurrent_queue.hpp"
#include "timed_concurrent_queue.hpp"

// Third-Party
#include "third_party/concurrentqueue.h"


template <class EventLoopBackend = EpollContext>
struct EventLoop {
    using Function   = std::function<void()>;  // TBD
    using TimeStamp  = int;                    // TBD
    using Identifier = int;                    // TBD
    void loop();
    void stop();

    void executeNow(Function func);
    void executeLater(Function func, Identifier identifier);
    void executeAt(TimeStamp, Function, Identifier identifier);
    void cancelExecution(Identifier identifier);
    void registerCallbackBeforeLoop(EventManager*);

    InterruptiveConcurrentQueue<FunctionType> immediateExecutionQueue;
    TimedConcurrentQueue<FunctionType> timedExecutionQueue;
    ConcurrentQueue<FunctionType> delayedExecutionQueue;

    EventLoopBackend eventLoopBackend;
};

#pragma once

// C++
#include <functional>

// First-party
// #include "scaler/io/ymq/event_manager.hpp"
// #include "scaler/io/ymq/interruptive_concurrent_queue.hpp"
// #include "scaler/io/ymq/timed_concurrent_queue.hpp"

// Third-Party
// #include "scaler/io/ymq/third_party/concurrentqueue.h"
// #include "scaler/io/ymq/event_loop_backend.hpp"

#include "scaler/io/ymq/epoll_context.h"
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

    // InterruptiveConcurrentQueue<FunctionType> immediateExecutionQueue;
    // TimedConcurrentQueue<FunctionType> timedExecutionQueue;
    // ConcurrentQueue<FunctionType> delayedExecutionQueue;

    // EventLoopBackend eventLoopBackend;
};

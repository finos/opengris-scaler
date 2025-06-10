#pragma once

// C++
#include <cstdint>  // uint64_t
#include <functional>

// First-party
// #include "scaler/io/ymq/event_manager.hpp"
// #include "scaler/io/ymq/interruptive_concurrent_queue.hpp"
// #include "scaler/io/ymq/timed_concurrent_queue.hpp"

// Third-Party
// #include "scaler/io/ymq/third_party/concurrentqueue.h"
// #include "scaler/io/ymq/event_loop_backend.hpp"

#include "scaler/io/ymq/epoll_context.h"
struct Timestamp;

template <class EventLoopBackend = EpollContext>
struct EventLoop {
    using Function   = std::function<void()>;  // TBD
    using Identifier = int;                    // TBD
    void loop() { eventLoopBackend.loop(); }
    void stop();

    void executeNow(Function func) { eventLoopBackend.executeNow(func); }
    void executeLater(Function func, Identifier identifier);
    void executeAt(Timestamp, Function, Identifier identifier);
    void cancelExecution(Identifier identifier);
    void registerCallbackBeforeLoop(EventManager*);

    void addFdToLoop(int fd, uint64_t events, EventManager* manager) {
        eventLoopBackend.addFdToLoop(fd, events, manager);
    }

    // InterruptiveConcurrentQueue<FunctionType> immediateExecutionQueue;
    // TimedConcurrentQueue<FunctionType> timedExecutionQueue;
    // ConcurrentQueue<FunctionType> delayedExecutionQueue;

    EventLoopBackend eventLoopBackend;
};

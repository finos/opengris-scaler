#pragma once

// System
#include <sys/epoll.h>

// C++
#include <functional>
#include <queue>

#include "scaler/io/ymq/configuration.h"
#include "scaler/io/ymq/timed_queue.h"

// First-party
#include "scaler/io/ymq/file_descriptor.h"
#include "scaler/io/ymq/interruptive_concurrent_queue.h"
#include "scaler/io/ymq/timestamp.h"

class EventManager;

// In the constructor, the epoll context should register eventfd/timerfd from
// This way, the queues need not know about the event manager. We don't use callbacks.
class EpollContext {
public:
    using Function             = Configuration::ExecutionFunction;
    using DelayedFunctionQueue = std::queue<Function>;
    using Identifier           = Configuration::ExecutionCancellationIdentifier;

    EpollContext() {
        _epfd = epoll_create1(0);
        epoll_event event {};

        event.events   = EPOLLIN | EPOLLET;
        event.data.u64 = _isInterruptiveFd;
        epoll_ctl(_epfd, EPOLL_CTL_ADD, _interruptiveFunctions.eventFd(), &event);

        event          = {};
        event.events   = EPOLLIN | EPOLLET;
        event.data.u64 = _isTimingFd;
        epoll_ctl(_epfd, EPOLL_CTL_ADD, _timingFunctions.timingFd(), &event);
    }

    ~EpollContext() {
        epoll_ctl(_epfd, EPOLL_CTL_DEL, _interruptiveFunctions.eventFd(), nullptr);
        epoll_ctl(_epfd, EPOLL_CTL_DEL, _timingFunctions.timingFd(), nullptr);

        close(_epfd);
    }

    void loop();

    int addFdToLoop(int fd, uint64_t events, EventManager* manager);
    void removeFdFromLoop(int fd);

    void executeNow(Function func) { _interruptiveFunctions.enqueue(std::move(func)); }
    void executeLater(Function func) { _delayedFunctions.emplace(std::move(func)); }
    Identifier executeAt(Timestamp timestamp, Function callback) {
        return _timingFunctions.push(timestamp, std::move(callback));
    }
    void cancelExecution(Identifier identifier) { _timingFunctions.cancelExecution(identifier); }

private:
    void execPendingFunctions();
    int _epfd;
    TimedQueue _timingFunctions;
    DelayedFunctionQueue _delayedFunctions;
    InterruptiveConcurrentQueue<Function> _interruptiveFunctions;
    static const size_t _isInterruptiveFd = 0;
    static const size_t _isTimingFd       = 1;
    static const size_t _reventSize       = 1024;
};

#pragma once
#ifdef __linux__

// System
#include <sys/epoll.h>

// C++
#include <functional>
#include <queue>

#include "scaler/io/ymq/configuration.h"
#include "scaler/io/ymq/timed_queue.h"

// First-party
#include "scaler/io/ymq/interruptive_concurrent_queue.h"
#include "scaler/io/ymq/timestamp.h"

namespace scaler {
namespace ymq {

class EventManager;

// In the constructor, the epoll context should register eventfd/timerfd from
// This way, the queues need not know about the event manager. We don't use callbacks.
class EpollContext {
public:
    using Function             = Configuration::ExecutionFunction;
    using DelayedFunctionQueue = std::queue<Function>;
    using Identifier           = Configuration::ExecutionCancellationIdentifier;

    EpollContext()
    {
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

    ~EpollContext()
    {
        epoll_ctl(_epfd, EPOLL_CTL_DEL, _interruptiveFunctions.eventFd(), nullptr);
        epoll_ctl(_epfd, EPOLL_CTL_DEL, _timingFunctions.timingFd(), nullptr);

        close(_epfd);
    }

    void loop();

    void addFdToLoop(int fd, uint64_t events, EventManager* manager);
    void removeFdFromLoop(int fd);

    // NOTE: Thread-safe method to communicate with the event loop thread
    void executeNow(Function func) { _interruptiveFunctions.enqueue(std::move(func)); }
    // WARN: NOT thread-safe. Thread safety is guaranteed by executeNow.
    void executeLater(Function func) { _delayedFunctions.emplace(std::move(func)); }
    // WARN: NOT thread-safe. Thread safety is guaranteed by executeNow.
    Identifier executeAt(Timestamp timestamp, Function callback)
    {
        return _timingFunctions.push(timestamp, std::move(callback));
    }
    void cancelExecution(Identifier identifier) { _timingFunctions.cancelExecution(identifier); }

private:
    void execPendingFunctions();
    int _epfd;
    TimedQueue _timingFunctions;
    DelayedFunctionQueue _delayedFunctions;
    InterruptiveConcurrentQueue<Function> _interruptiveFunctions;
    constexpr static const size_t _isInterruptiveFd = 0;
    constexpr static const size_t _isTimingFd       = 1;
    constexpr static const size_t _reventSize       = 1024;
};

}  // namespace ymq
}  // namespace scaler

#endif  // __linux__

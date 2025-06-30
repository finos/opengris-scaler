#pragma once

// System
#include <sys/epoll.h>

// C++
#include <expected>
#include <functional>
#include <queue>

#include "scaler/io/ymq/timed_queue.h"

// First-party
#include "scaler/io/ymq/file_descriptor.h"
#include "scaler/io/ymq/interruptive_concurrent_queue.h"
#include "scaler/io/ymq/timestamp.h"

class EventManager;

// struct EpollContext {
//     FileDescriptor epoll_fd;
//     TimedQueue timingFunctions;
//
//     using DelayedFunctionQueue = std::queue<std::function<void()>>;
//     DelayedFunctionQueue delayedFunctions;
//
//     using Function   = std::function<void()>;  // TBD
//     using Identifier = int;                    // TBD
//     void registerCallbackBeforeLoop(EventManager*);
//
//     EpollContext() {
//         auto fd = FileDescriptor::epollfd();
//
//         if (!fd) {
//             throw std::system_error(fd.error(), std::system_category(), "Failed to create epoll fd");
//         }
//
//         this->epoll_fd = std::move(*fd);
//         timingFunctions.onCreated();
//     }
//
//     void loop();
//     void registerEventManager(EventManager& em);
//     void removeEventManager(EventManager& em);
//
//     void stop();
//
//     void executeNow(Function func) {
//     }
//
//     void executeLater(Function func, Identifier) { delayedFunctions.emplace(std::move(func)); }
//
//     void executeAt(Timestamp timestamp, Function callback) { timingFunctions.push(timestamp, callback); }
//
//     bool cancelExecution(Identifier identifier);
//
//     void execPendingFunctions();
//
//     void addFdToLoop(int fd, uint64_t events, EventManager* manager);
//
//     // int connect_timer_tfd;
//     // std::map<int, EventManager*> monitoringEvent;
//     // bool timer_armed;
//     // // NOTE: Utility functions, may be defined otherwise
//     // void ensure_timer_armed();
//     // void remove_epoll(int fd);
//     // EpollData* epoll_by_fd(int fd);
// };

// In the constructor, the epoll context should register eventfd/timerfd from
// This way, the queues need not know about the event manager. We don't use callbacks.
class EpollContext {
    using Function             = std::function<void()>;
    using DelayedFunctionQueue = std::queue<std::function<void()>>;
    int _epfd;
    TimedQueue _timingFunctions;
    DelayedFunctionQueue _delayedFunctions;
    InterruptiveConcurrentQueue<Function> _interruptiveFunctions;
    static const size_t _isInterruptiveFd = 0;
    static const size_t _isTimingFd       = 1;

public:
    using Identifier = int;  // TBD

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
    void stop();

    void registerEventManager(EventManager& em);
    void removeEventManager(EventManager& em);

    void executeNow(Function func) { _interruptiveFunctions.enqueue(std::move(func)); }
    void executeLater(Function func) { _delayedFunctions.emplace(std::move(func)); }

    Identifier executeAt(Timestamp timestamp, Function callback) {
        return _timingFunctions.push(timestamp, std::move(callback));
    }

    void cancelExecution(Identifier identifier) { _timingFunctions.cancelExecution(identifier); }

    void execPendingFunctions();

    int addFdToLoop(int fd, uint64_t events, EventManager* manager);
    void removeFdFromLoop(int fd);
};

#pragma once

// C++
#include <sys/epoll.h>

#include <cstdint>  // uint64_t
#include <functional>

#include "scaler/io/ymq/timestamp.h"

// First-party

class EventManager;

struct EpollContext {
    int epfd;

    using Function   = std::function<void()>;  // TBD
    using Identifier = int;                    // TBD
    void registerCallbackBeforeLoop(EventManager*);

    EpollContext() { epfd = epoll_create1(0); }

    void loop();
    void addFdToLoop(int fd, uint64_t events, EventManager* manager);

    void stop();

    void executeNow(Function func) {
        // TODO: Implement this function
    }

    void executeLater(Function func, Identifier identifier);
    void executeAt(Timestamp, Function, Identifier identifier);
    void cancelExecution(Identifier identifier);

    void executePendingFunctors();

    // int connect_timer_tfd;
    // std::map<int, EventManager*> monitoringEvent;
    // bool timer_armed;
    // // NOTE: Utility functions, may be defined otherwise
    // void ensure_timer_armed();
    // void remove_epoll(int fd);
    // EpollData* epoll_by_fd(int fd);
};

#pragma once

// System
#include <sys/epoll.h>

// C++
#include <functional>
#include <system_error>

// First-party
#include "scaler/io/ymq/event_manager.h"
#include "scaler/io/ymq/file_descriptor.h"
#include "scaler/io/ymq/timestamp.h"

class EventManager;

struct EpollContext {
    FileDescriptor epoll_fd;

    using Function   = std::function<void()>;  // TBD
    using Identifier = int;                    // TBD
    void registerCallbackBeforeLoop(EventManager*);

    EpollContext() {
        auto fd = FileDescriptor::epollfd();

        if (!fd) {
            throw std::system_error(fd.error(), std::system_category(), "Failed to create epoll fd");
        }

        this->epoll_fd = std::move(*fd);
    }

    void loop();
    void registerEventManager(EventManager& em);
    void removeEventManager(EventManager& em);

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

#include "scaler/io/ymq/epoll_context.h"

#include <sys/epoll.h>

#include <cerrno>
#include <functional>

#include "scaler/io/ymq/event_manager.h"

namespace scaler {
namespace ymq {

void EpollContext::execPendingFunctions() {
    while (_delayedFunctions.size()) {
        auto top = std::move(_delayedFunctions.front());
        top();
        _delayedFunctions.pop();
    }
}

void EpollContext::loop() {
    std::array<epoll_event, _reventSize> events {};
    int n = epoll_wait(_epfd, events.data(), _reventSize, -1);
    if (n == -1) {
        printf("EPOWAIT Something wrong, errno %d, strerror = %s\n", errno, strerror(errno));
        exit(1);
    }

    for (auto it = events.begin(); it != events.begin() + n; ++it) {
        epoll_event current_event = *it;
        auto* event               = (EventManager*)current_event.data.ptr;
        if (event == (void*)_isInterruptiveFd) {
            auto vec = _interruptiveFunctions.dequeue();
            std::ranges::for_each(vec, [](auto&& x) { x(); });
        } else if (event == (void*)_isTimingFd) {
            auto vec = _timingFunctions.dequeue();
            std::ranges::for_each(vec, [](auto& x) { x(); });
        } else {
            event->onEvents(current_event.events);
        }
    }

    execPendingFunctions();
}

int EpollContext::addFdToLoop(int fd, uint64_t events, EventManager* manager) {
    epoll_event event {};
    event.events   = (int)events & (EPOLLIN | EPOLLOUT | EPOLLET);
    event.data.ptr = (void*)manager;
    int res        = epoll_ctl(_epfd, EPOLL_CTL_ADD, fd, &event);
    if (res == 0)
        return 0;

    if (errno == EEXIST) {
        if (epoll_ctl(_epfd, EPOLL_CTL_MOD, fd, &event) == 0) {
            return 0;
        } else {
            return errno;
        }
    } else {
        return errno;
    }
}

void EpollContext::removeFdFromLoop(int fd) {
    if (epoll_ctl(_epfd, EPOLL_CTL_DEL, fd, nullptr) == -1) {
        // TODO: Update when error type is determined
        panic("Failed to remove fd from epoll loop");
    }
}

}  // namespace ymq
}  // namespace scaler

#ifdef __linux__

#include "scaler/io/ymq/epoll_context.h"

#include <sys/epoll.h>

#include <cerrno>
#include <functional>

#include "scaler/io/ymq/error.h"
#include "scaler/io/ymq/event_manager.h"

namespace scaler {
namespace ymq {

void EpollContext::execPendingFunctions()
{
    while (_delayedFunctions.size()) {
        auto top = std::move(_delayedFunctions.front());
        top();
        _delayedFunctions.pop();
    }
}

void EpollContext::loop()
{
    std::array<epoll_event, _reventSize> events {};
    int n = epoll_wait(_epfd, events.data(), _reventSize, -1);
    if (n == -1) {
        const int myErrno = errno;
        switch (myErrno) {
            case EINTR:
                // unrecoverableError({
                //     Error::ErrorCode::SignalNotSupported,
                //     "Originated from",
                //     "epoll_wait(2)",
                //     "Errno is",
                //     strerror(errno),
                // });

                // todo: investigate better error handling
                // the epoll thread is not expected to receive signals(?)
                // but occasionally does (e.g. sigwinch) and we shouldn't stop the thread in that case
                break;
            case EBADF:
            case EFAULT:
            case EINVAL:
            default:
                unrecoverableError({
                    Error::ErrorCode::CoreBug,
                    "Originated from",
                    "epoll_wait(2)",
                    "Errno is",
                    strerror(errno),
                    "_epfd",
                    _epfd,
                    "_reventSize",
                    _reventSize,
                });
                break;
        }
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

void EpollContext::addFdToLoop(int fd, uint64_t events, EventManager* manager)
{
    epoll_event event {};
    event.events   = (int)events & (EPOLLIN | EPOLLOUT | EPOLLET);
    event.data.ptr = (void*)manager;
    int res        = epoll_ctl(_epfd, EPOLL_CTL_ADD, fd, &event);
    if (res == 0) {
        return;
    }

    if (errno == EEXIST) {
        if (epoll_ctl(_epfd, EPOLL_CTL_MOD, fd, &event) == 0) {
            return;
        }
    }

    const int myErrno = errno;
    switch (myErrno) {
        case ENOMEM:
        case ENOSPC:
            unrecoverableError({
                Error::ErrorCode::ConfigurationError,
                "Originated from",
                "epoll_ctl(2)",
                "Errno is",
                strerror(errno),
                "_epfd",
                _epfd,
                "fd",
                fd,
                "event.events",
                (int)event.events,
            });
            break;

        case EBADF:
        case EPERM:
        case EINVAL:
        case ELOOP:
        case ENOENT:
        default:
            unrecoverableError({
                Error::ErrorCode::CoreBug,
                "Originated from",
                "epoll_ctl(2)",
                "Errno is",
                strerror(errno),
                "_epfd",
                _epfd,
                "fd",
                fd,
                "event.events",
                (int)event.events,
            });
            break;
    }
}

void EpollContext::removeFdFromLoop(int fd)
{
    if (epoll_ctl(_epfd, EPOLL_CTL_DEL, fd, nullptr) == 0) {
        return;
    }
    const int myErrno = errno;
    switch (myErrno) {
        case ENOMEM:
        case ENOSPC:
            unrecoverableError({
                Error::ErrorCode::ConfigurationError,
                "Originated from",
                __PRETTY_FUNCTION__,
                "Errno is",
                strerror(errno),
                "_epfd",
                _epfd,
                "fd",
                fd,
            });
            break;

        case EBADF:
        case EPERM:
        case EINVAL:
        case ELOOP:
        case ENOENT:
        default:
            unrecoverableError({
                Error::ErrorCode::CoreBug,
                "Originated from",
                __PRETTY_FUNCTION__,
                "Errno is",
                strerror(errno),
                "_epfd",
                _epfd,
                "fd",
                fd,
            });
            break;
    }
}

}  // namespace ymq
}  // namespace scaler

#endif  // __linux__

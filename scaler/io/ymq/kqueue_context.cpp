#ifdef __APPLE__

#include <sys/event.h>
#include <sys/types.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstring>
#include <functional>
#include <ranges>

#include "scaler/io/ymq/kqueue_context.h"
#include "scaler/io/ymq/error.h"
#include "scaler/io/ymq/event_manager.h"

namespace scaler {
namespace ymq {

namespace {

int createKqueueOrPanic(const char* origin)
{
    const int fd = kqueue();
    if (fd == -1) {
        const int myErrno = errno;
        switch (myErrno) {
            case EMFILE:
            case ENFILE:
            case ENOMEM:
                unrecoverableError({
                    Error::ErrorCode::ConfigurationError,
                    "Originated from",
                    origin,
                    "Errno is",
                    strerror(myErrno),
                });
                break;

            default:
                unrecoverableError({
                    Error::ErrorCode::CoreBug,
                    "Originated from",
                    origin,
                    "Errno is",
                    strerror(myErrno),
                });
                break;
        }
    }
    return fd;
}

int64_t clampMicroseconds(int64_t micros)
{
    return micros < 0 ? 0 : micros;
}

}  // namespace

// TimedQueue implementation for macOS
TimedQueue::TimedQueue(int kqueueFd, uintptr_t ident)
    : _kqfd(kqueueFd)
    , _ident(ident)
    , _currentId {}
{
    if (_kqfd < 0) {
        unrecoverableError({
            Error::ErrorCode::CoreBug,
            "Originated from",
            "TimedQueue::TimedQueue",
            "Errno is",
            "invalid kqueue fd",
        });
    }
}

TimedQueue::~TimedQueue() = default;

void TimedQueue::armNextTimer()
{
    struct kevent kev;

    if (pq.empty()) {
        EV_SET(&kev, _ident, EVFILT_TIMER, EV_DELETE, 0, 0, nullptr);
        if (kevent(_kqfd, &kev, 1, nullptr, 0, nullptr) == -1) {
            if (errno != ENOENT) {
                unrecoverableError({
                    Error::ErrorCode::CoreBug,
                    "Originated from",
                    "kevent(2) - disarm timer",
                    "Errno is",
                    strerror(errno),
                });
            }
        }
        return;
    }

    const auto microsecs = clampMicroseconds(convertToKqueueTimer(std::get<0>(pq.top())));
    EV_SET(&kev, _ident, EVFILT_TIMER, EV_ADD | EV_ENABLE | EV_ONESHOT, NOTE_USECONDS, microsecs, nullptr);

    if (kevent(_kqfd, &kev, 1, nullptr, 0, nullptr) == -1) {
        unrecoverableError({
            Error::ErrorCode::CoreBug,
            "Originated from",
            "kevent(2) - arm timer",
            "Errno is",
            strerror(errno),
        });
    }
}

TimedQueue::Identifier TimedQueue::push(Timestamp timestamp, Callback cb)
{
    const auto id = _currentId++;
    pq.push({timestamp, std::move(cb), id});
    armNextTimer();
    return id;
}

std::vector<TimedQueue::Callback> TimedQueue::dequeue()
{
    std::vector<Callback> callbacks;
    Timestamp now;

    while (!pq.empty()) {
        if (std::get<0>(pq.top()) < now) {
            auto [ts, cb, id] = std::move(const_cast<PriorityQueue::reference>(pq.top()));
            pq.pop();
            auto cancelled = _cancelledFunctions.find(id);
            if (cancelled != _cancelledFunctions.end()) {
                _cancelledFunctions.erase(cancelled);
            } else {
                callbacks.emplace_back(std::move(cb));
            }
        } else {
            break;
        }
    }

    armNextTimer();

    return callbacks;
}

// InterruptiveConcurrentQueue implementation for macOS
template <typename T>
void InterruptiveConcurrentQueue<T>::enqueue(T item)
{
    _queue.enqueue(std::move(item));

    // Trigger a user event on the kqueue to wake up the event loop
    struct kevent kev;
    EV_SET(&kev, _ident, EVFILT_USER, 0, NOTE_TRIGGER, 0, nullptr);

    if (kevent(_kqfd, &kev, 1, nullptr, 0, nullptr) == -1) {
        unrecoverableError({
            Error::ErrorCode::CoreBug,
            "Originated from",
            "kevent(2) - trigger user event",
            "Errno is",
            strerror(errno),
        });
    }
}

template <typename T>
std::vector<T> InterruptiveConcurrentQueue<T>::dequeue()
{
    std::vector<T> vecT;
    while (true) {
        T next;
        if (!_queue.try_dequeue(next)) {
            break;
        }
        vecT.emplace_back(std::move(next));
    }

    return vecT;
}

// Explicit template instantiation for Function type
template class InterruptiveConcurrentQueue<Configuration::ExecutionFunction>;

// KqueueContext implementation
KqueueContext::KqueueContext()
    : _kqfd(createKqueueOrPanic("kqueue(2)"))
    , _timingFunctions(_kqfd, _isTimingFd)
    , _delayedFunctions()
    , _interruptiveFunctions(_kqfd, _isInterruptiveFd)
{

    // Register user event for interruptive functions
    struct kevent kev;
    EV_SET(&kev, _isInterruptiveFd, EVFILT_USER, EV_ADD | EV_ENABLE | EV_CLEAR, 0, 0, nullptr);

    if (kevent(_kqfd, &kev, 1, nullptr, 0, nullptr) == -1) {
        unrecoverableError({
            Error::ErrorCode::CoreBug,
            "Originated from",
            "kevent(2) - register user event",
            "Errno is",
            strerror(errno),
        });
    }
}

KqueueContext::~KqueueContext()
{
    if (_kqfd >= 0)
        close(_kqfd);
}

void KqueueContext::execPendingFunctions()
{
    while (!_delayedFunctions.empty()) {
        auto top = std::move(_delayedFunctions.front());
        top();
        _delayedFunctions.pop();
    }
}

void KqueueContext::loop()
{
    std::array<struct kevent, _reventSize> events {};

    int n = kevent(_kqfd, nullptr, 0, events.data(), _reventSize, nullptr);
    if (n == -1) {
        const int myErrno = errno;
        switch (myErrno) {
            case EINTR:
                // Signal interrupted the wait, just return and try again
                return;

            case EBADF:
            case EFAULT:
            case EINVAL:
            default:
                unrecoverableError({
                    Error::ErrorCode::CoreBug,
                    "Originated from",
                    "kevent(2)",
                    "Errno is",
                    strerror(myErrno),
                    "_kqfd",
                    _kqfd,
                    "_reventSize",
                    _reventSize,
                });
                break;
        }
    }

    for (int i = 0; i < n; ++i) {
        const struct kevent& current_event = events[i];

        if (current_event.filter == EVFILT_USER && current_event.ident == _isInterruptiveFd) {
            // Handle interruptive functions
            auto vec = _interruptiveFunctions.dequeue();
            std::ranges::for_each(vec, [](auto&& x) { x(); });
        } else if (current_event.filter == EVFILT_TIMER) {
            // Handle timing functions
            auto vec = _timingFunctions.dequeue();
            std::ranges::for_each(vec, [](auto& x) { x(); });
        } else {
            // Handle socket events
            auto* event = (EventManager*)current_event.udata;
            if (event) {
                uint64_t ymq_events = 0;

                // Convert kqueue events to our event flags
                if (current_event.filter == EVFILT_READ) {
                    ymq_events |= KQUEUE_EVENT_READ;
                }
                if (current_event.filter == EVFILT_WRITE) {
                    ymq_events |= KQUEUE_EVENT_WRITE;
                }
                if (current_event.flags & EV_EOF) {
                    // Socket closed
                    ymq_events |= KQUEUE_EVENT_CLOSE;
                }
                if (current_event.flags & EV_ERROR) {
                    ymq_events |= KQUEUE_EVENT_ERROR;
                }

                event->onEvents(ymq_events);
            }
        }
    }

    execPendingFunctions();
}

void KqueueContext::addFdToLoop(int fd, uint64_t events, EventManager* manager)
{
    std::vector<struct kevent> changes;

    // Add read filter if requested
    if (events & EVFILT_READ) {
        struct kevent kev;
        EV_SET(&kev, fd, EVFILT_READ, EV_ADD | EV_ENABLE, 0, 0, manager);
        changes.push_back(kev);
    }

    // Add write filter if requested
    if (events & EVFILT_WRITE) {
        struct kevent kev;
        EV_SET(&kev, fd, EVFILT_WRITE, EV_ADD | EV_ENABLE, 0, 0, manager);
        changes.push_back(kev);
    }

    if (!changes.empty()) {
        if (kevent(_kqfd, changes.data(), changes.size(), nullptr, 0, nullptr) == -1) {
            const int myErrno = errno;
            switch (myErrno) {
                case ENOMEM:
                    unrecoverableError({
                        Error::ErrorCode::ConfigurationError,
                        "Originated from",
                        "kevent(2) - addFdToLoop",
                        "Errno is",
                        strerror(myErrno),
                        "_kqfd",
                        _kqfd,
                        "fd",
                        fd,
                    });
                    break;

                case EBADF:
                case EINVAL:
                case ENOENT:
                case ESRCH:
                default:
                    unrecoverableError({
                        Error::ErrorCode::CoreBug,
                        "Originated from",
                        "kevent(2) - addFdToLoop",
                        "Errno is",
                        strerror(myErrno),
                        "_kqfd",
                        _kqfd,
                        "fd",
                        fd,
                    });
                    break;
            }
        }
    }
}

void KqueueContext::removeFdFromLoop(int fd)
{
    std::vector<struct kevent> changes;

    // Remove both read and write filters
    struct kevent kev_read;
    EV_SET(&kev_read, fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
    changes.push_back(kev_read);

    struct kevent kev_write;
    EV_SET(&kev_write, fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
    changes.push_back(kev_write);

    // It's OK if one of them fails (might not be registered), so we don't check error
    kevent(_kqfd, changes.data(), changes.size(), nullptr, 0, nullptr);
}

}  // namespace ymq
}  // namespace scaler

#endif  // __APPLE__

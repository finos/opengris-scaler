#pragma once

#include <sys/timerfd.h>
#include <unistd.h>

#include <cassert>
#include <queue>
#include <set>

#include "scaler/io/ymq/configuration.h"
#include "scaler/io/ymq/error.h"
#include "scaler/io/ymq/timestamp.h"

namespace scaler {
namespace ymq {

inline int createTimerfd()
{
    int timerfd = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    if (timerfd >= 0) {
        return timerfd;
    }

    const int myErrno = errno;
    switch (myErrno) {
        case EMFILE:
        case ENFILE:
        case ENODEV:
        case ENOMEM:
        case EPERM:
            unrecoverableError({
                Error::ErrorCode::ConfigurationError,
                "Originated from",
                __PRETTY_FUNCTION__,
                "Errno is",
                strerror(myErrno),
            });
            break;

        case EINVAL:
        case EBADF:
        case EFAULT:
        case ECANCELED:
        default:
            unrecoverableError({
                Error::ErrorCode::CoreBug,
                "Originated from",
                __PRETTY_FUNCTION__,
                "Errno is",
                strerror(myErrno),
            });
            break;
    }
}

class TimedQueue {
public:
    using Callback            = Configuration::TimedQueueCallback;
    using Identifier          = Configuration::ExecutionCancellationIdentifier;
    using TimedFunc           = std::tuple<Timestamp, Callback, Identifier>;
    constexpr static auto cmp = [](const auto& x, const auto& y) { return std::get<0>(x) < std::get<0>(y); };
    using PriorityQueue       = std::priority_queue<TimedFunc, std::vector<TimedFunc>, decltype(cmp)>;

    TimedQueue(): _timerFd(createTimerfd()), _currentId {} { assert(_timerFd); }
    ~TimedQueue()
    {
        if (_timerFd >= 0)
            close(_timerFd);
    }

    Identifier push(Timestamp timestamp, Callback cb)
    {
        auto ts = convertToItimerspec(timestamp);
        if (pq.empty() || timestamp < std::get<0>(pq.top())) {
            int ret = timerfd_settime(_timerFd, 0, &ts, nullptr);
            if (ret == -1) {
                unrecoverableError({
                    Error::ErrorCode::CoreBug,
                    "Originated from",
                    __PRETTY_FUNCTION__,
                    "Errno is",
                    strerror(errno),
                });
            }
        }
        pq.push({timestamp, std::move(cb), _currentId});
        return _currentId++;
    }

    void cancelExecution(Identifier id) { _cancelledFunctions.insert(id); }

    std::vector<Callback> dequeue()
    {
        uint64_t numItems;
        ssize_t n = read(_timerFd, &numItems, sizeof numItems);
        if (n != sizeof numItems) [[unlikely]] {
            // This should never happen anyway
            unrecoverableError({
                Error::ErrorCode::CoreBug,
                "Originated from",
                __PRETTY_FUNCTION__,
                "Errno is",
                strerror(errno),
            });
        }

        std::vector<Callback> callbacks;

        Timestamp now;
        while (pq.size()) {
            if (std::get<0>(pq.top()) < now) {
                auto [ts, cb, id] = std::move(const_cast<PriorityQueue::reference>(pq.top()));
                pq.pop();
                auto cancelled = _cancelledFunctions.find(id);
                if (cancelled != _cancelledFunctions.end()) {
                    _cancelledFunctions.erase(cancelled);
                } else {
                    callbacks.emplace_back(std::move(cb));
                }
            } else
                break;
        }

        if (!pq.empty()) {
            auto nextTs = std::get<0>(pq.top());
            auto ts     = convertToItimerspec(nextTs);
            int ret     = timerfd_settime(_timerFd, 0, &ts, nullptr);
            if (ret == -1) {
                const int myErrno = errno;
                switch (myErrno) {
                    case EMFILE:
                    case ENFILE:
                    case ENODEV:
                    case ENOMEM:
                    case EPERM:
                        unrecoverableError({
                            Error::ErrorCode::ConfigurationError,
                            "Originated from",
                            __PRETTY_FUNCTION__,
                            "Errno is",
                            strerror(myErrno),
                        });
                        break;

                    case EINVAL:
                    case EBADF:
                    case EFAULT:
                    case ECANCELED:
                    default:
                        unrecoverableError({
                            Error::ErrorCode::CoreBug,
                            "Originated from",
                            __PRETTY_FUNCTION__,
                            "Errno is",
                            strerror(myErrno),
                        });
                        break;
                }
            }
        }
        return callbacks;
    }

    int timingFd() const { return _timerFd; }

private:
    int _timerFd;
    Identifier _currentId;
    PriorityQueue pq;
    std::set<Identifier> _cancelledFunctions;
};

}  // namespace ymq
}  // namespace scaler

#pragma once

#include <cassert>
#include <queue>
#include <set>

#include "scaler/error/error.h"
#include "scaler/ymq/configuration.h"
#include "scaler/ymq/timestamp.h"

#ifdef __linux__
#include <sys/eventfd.h>
#include <sys/timerfd.h>
#endif

#ifdef _WIN32
// clang-format off
#define NOMINMAX
#include <windows.h>
#include <winsock2.h>
#include <mswsock.h>
#include <ws2tcpip.h> // inet_pton
// clang-format on
#endif  // _WIN32

// Windows being evil
#ifdef _WIN32
#undef SendMessageCallback
#define __PRETTY_FUNCTION__ __FUNCSIG__
#define EPOLLIN             (0)
#define EPOLLOUT            (0)
#define EPOLLET             (0)
#endif  // _WIN32

namespace scaler {
namespace ymq {

struct TimedCallback {
    Timestamp timestamp;
    Configuration::TimedQueueCallback callback;
    Configuration::ExecutionCancellationIdentifier identifier;

    constexpr bool operator<(const TimedCallback& other) const { return timestamp < other.timestamp; }
};

#ifdef __linux__
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
    TimedQueue(): _timerFd(createTimerfd()), _currentId {} { assert(_timerFd); }
    ~TimedQueue()
    {
        if (_timerFd >= 0)
            close(_timerFd);
    }

    Configuration::ExecutionCancellationIdentifier push(Timestamp timestamp, Configuration::TimedQueueCallback cb)
    {
        auto ts = convertToItimerspec(timestamp);
        if (pq.empty() || timestamp < pq.top().timestamp) {
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

    void cancelExecution(Configuration::ExecutionCancellationIdentifier id) { _cancelledFunctions.insert(id); }

    std::vector<Configuration::TimedQueueCallback> dequeue()
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

        std::vector<Configuration::TimedQueueCallback> callbacks;

        Timestamp now;
        while (pq.size()) {
            if (pq.top().timestamp < now) {
                auto [ts, cb, id] = std::move(const_cast<std::priority_queue<TimedCallback>::reference>(pq.top()));
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
            auto nextTs = pq.top().timestamp;
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
    Configuration::ExecutionCancellationIdentifier _currentId;
    std::priority_queue<TimedCallback> pq;
    std::set<Configuration::ExecutionCancellationIdentifier> _cancelledFunctions;
};

#endif  // __linux__

#ifdef _WIN32
class TimedQueue {
public:
    HANDLE _completionPort;
    const size_t _key;

    // TODO: Handle error for system calls
    TimedQueue(HANDLE completionPort, size_t key)
        : _completionPort(completionPort), _key(key), _timerFd(CreateWaitableTimer(NULL, 0, NULL)), _currentId {}
    {
        assert(_timerFd);
    }
    ~TimedQueue()
    {
        if (_timerFd) {
            CloseHandle(_timerFd);
        }
    }

    Configuration::ExecutionCancellationIdentifier push(Timestamp timestamp, Configuration::TimedQueueCallback cb)
    {
        auto ts = convertToLARGE_INTEGER(timestamp);
        if (pq.empty() || timestamp < pq.top().timestamp) {
            SetWaitableTimer(
                _timerFd,
                (LARGE_INTEGER*)&ts,
                0,
                [](LPVOID thisPointer, DWORD, DWORD) {
                    auto* self = (TimedQueue*)thisPointer;
                    PostQueuedCompletionStatus(self->_completionPort, 0, (ULONG_PTR)(self->_timerFd), nullptr);
                },
                (LPVOID)this,
                false);
        }
        pq.push({timestamp, std::move(cb), _currentId});
        return _currentId++;
    }

    void cancelExecution(Configuration::ExecutionCancellationIdentifier id) { _cancelledFunctions.insert(id); }

    std::vector<Configuration::TimedQueueCallback> dequeue()
    {
        std::vector<Configuration::TimedQueueCallback> callbacks;

        Timestamp now;
        while (pq.size()) {
            if (pq.top().timestamp < now) {
                auto [ts, cb, id] = std::move(const_cast<std::priority_queue<TimedCallback>::reference>(pq.top()));
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
            auto nextTs = pq.top().timestamp;
            auto ts     = convertToLARGE_INTEGER(nextTs);
            SetWaitableTimer(
                _timerFd,
                (LARGE_INTEGER*)&ts,
                0,
                [](LPVOID thisPointer, DWORD, DWORD) {
                    auto* self = (TimedQueue*)thisPointer;
                    PostQueuedCompletionStatus(self->_completionPort, 0, (ULONG_PTR)(self->_timerFd), nullptr);
                },
                (LPVOID)this,
                false);
        }
        return callbacks;
    }

private:
    HANDLE _timerFd;
    Configuration::ExecutionCancellationIdentifier _currentId;
    std::priority_queue<TimedCallback> pq;
    std::set<Configuration::ExecutionCancellationIdentifier> _cancelledFunctions;
};

#endif  // _WIN32

}  // namespace ymq
}  // namespace scaler

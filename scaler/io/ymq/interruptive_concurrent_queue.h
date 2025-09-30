#pragma once

#ifdef __linux__
#include <sys/eventfd.h>
#include <unistd.h>
#endif  // __linux__
#ifdef _WIN32
// clang-format off
#include <windows.h>
#include <winsock2.h>
// clang-format on
#endif  // _WIN32
#ifdef __APPLE__
#include <sys/event.h>
#include <cerrno>
#include <cstring>
#endif  // __APPLE__

#include <vector>

#include "scaler/io/ymq/error.h"
#include "third_party/concurrentqueue.h"

namespace scaler {
namespace ymq {

class EventManager;

#ifdef __linux__
template <typename T>
class InterruptiveConcurrentQueue {
public:
    InterruptiveConcurrentQueue(): _queue()
    {
        _eventFd = eventfd(0, EFD_NONBLOCK);
        if (_eventFd == -1) {
            const int myErrno = errno;
            switch (myErrno) {
                case ENFILE:
                case ENODEV:
                case ENOMEM:
                case EMFILE:
                    unrecoverableError({
                        Error::ErrorCode::ConfigurationError,
                        "Originated from",
                        "eventfd(2)",
                        "Errno is",
                        strerror(myErrno),
                    });
                    break;

                case EINVAL:
                default:
                    unrecoverableError({
                        Error::ErrorCode::CoreBug,
                        "Originated from",
                        "eventfd(2)",
                        "Errno is",
                        strerror(myErrno),
                        "flags",
                        "EFD_NONBLOCK",
                    });
                    break;
            }
        }
    }

    int eventFd() const { return _eventFd; }

    void enqueue(T item)
    {
        _queue.enqueue(std::move(item));

        uint64_t u = 1;
        if (::eventfd_write(_eventFd, u) < 0) {
            unrecoverableError({
                Error::ErrorCode::CoreBug,
                "Originated from",
                "eventfd_write(2)",
                "Errno is",
                strerror(errno),
            });
        }
    }

    // NOTE: this method will block until an item is available
    std::vector<T> dequeue()
    {
        uint64_t u {};
        if (::eventfd_read(_eventFd, &u) < 0) {
            if (errno == EAGAIN) {
                return {};
            }

            unrecoverableError({
                Error::ErrorCode::CoreBug,
                "Originated from",
                "eventfd_read(2)",
                "Errno is",
                strerror(errno),
            });
        }

        std::vector<T> vecT(u);
        for (auto i = 0uz; i < u; ++i) {
            while (!_queue.try_dequeue(vecT[i]))
                ;
        }
        return vecT;
    }

    // unmovable, uncopyable
    InterruptiveConcurrentQueue(const InterruptiveConcurrentQueue&)            = delete;
    InterruptiveConcurrentQueue& operator=(const InterruptiveConcurrentQueue&) = delete;
    InterruptiveConcurrentQueue(InterruptiveConcurrentQueue&&)                 = delete;
    InterruptiveConcurrentQueue& operator=(InterruptiveConcurrentQueue&&)      = delete;

    ~InterruptiveConcurrentQueue() { close(_eventFd); }

private:
    int _eventFd;
    moodycamel::ConcurrentQueue<T> _queue;
};
#endif  // __linux__

#ifdef _WIN32
template <typename T>
class InterruptiveConcurrentQueue {
public:
    InterruptiveConcurrentQueue(HANDLE completionPort, size_t key): _queue(), _completionPort(completionPort), _key(key)
    {
    }

    void enqueue(T item)
    {
        _queue.enqueue(std::move(item));
        if (!PostQueuedCompletionStatus(_completionPort, 0, (ULONG_PTR)_key, nullptr)) {
            unrecoverableError({
                Error::ErrorCode::CoreBug,
                "Originated from",
                "PostQueuedCompletionStatus",
                "Errno is",
                GetLastError(),
            });
        }
    }

    // NOTE: this method will block until an item is available
    // NOTE: On Windows, due to how the polling mechanism work, we can only do it 1 by 1.
    // NOTE: On Windows, we don't have a guaranteed number of functions, unlike with eventfd.
    std::vector<T> dequeue()
    {
        std::vector<T> vecT(1);
        while (!_queue.try_dequeue(vecT[0]))
            ;
        return vecT;
    }

    // unmovable, uncopyable
    InterruptiveConcurrentQueue(const InterruptiveConcurrentQueue&)            = delete;
    InterruptiveConcurrentQueue& operator=(const InterruptiveConcurrentQueue&) = delete;
    InterruptiveConcurrentQueue(InterruptiveConcurrentQueue&&)                 = delete;
    InterruptiveConcurrentQueue& operator=(InterruptiveConcurrentQueue&&)      = delete;

private:
    HANDLE _completionPort;
    const size_t _key;
    moodycamel::ConcurrentQueue<T> _queue;
};

#endif  // _WIN32

#ifdef __APPLE__
template <typename T>
class InterruptiveConcurrentQueue {
public:
    InterruptiveConcurrentQueue(int kqfd, uintptr_t ident): _kqfd(kqfd), _ident(ident) {}

    uintptr_t ident() const { return _ident; }

    void enqueue(T item);
    std::vector<T> dequeue();

    // unmovable, uncopyable
    InterruptiveConcurrentQueue(const InterruptiveConcurrentQueue&)            = delete;
    InterruptiveConcurrentQueue& operator=(const InterruptiveConcurrentQueue&) = delete;
    InterruptiveConcurrentQueue(InterruptiveConcurrentQueue&&)                 = delete;
    InterruptiveConcurrentQueue& operator=(InterruptiveConcurrentQueue&&)      = delete;

private:
    int _kqfd;  // shared kqueue descriptor
    uintptr_t _ident;  // EVFILT_USER ident reserved for this queue
    moodycamel::ConcurrentQueue<T> _queue;
};
#endif  // __APPLE__

}  // namespace ymq
}  // namespace scaler

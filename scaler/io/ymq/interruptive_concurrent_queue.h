#pragma once

#include <sys/eventfd.h>
#include <unistd.h>

// C++
#include <cstdlib>
#include <vector>

#include "scaler/io/ymq/error.h"
#include "third_party/concurrentqueue.h"

namespace scaler {
namespace ymq {

class EventManager;

template <typename T>
class InterruptiveConcurrentQueue {
    int _eventFd;
    moodycamel::ConcurrentQueue<T> _queue;

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
};

}  // namespace ymq
}  // namespace scaler

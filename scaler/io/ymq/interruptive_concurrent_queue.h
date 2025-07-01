#pragma once

#include <sys/eventfd.h>

// C++
#include <cstdlib>
#include <vector>

#include "third_party/concurrentqueue.h"

using moodycamel::ConcurrentQueue;

class EventManager;

template <typename T>
class InterruptiveConcurrentQueue {
    int _eventFd;
    ConcurrentQueue<T> _queue;

public:
    InterruptiveConcurrentQueue(): _queue() {
        _eventFd = eventfd(0, EFD_NONBLOCK);
        if (_eventFd == -1) {
            printf("eventfd goes wrong\n");
            exit(1);
        }
    }

    int eventFd() const { return _eventFd; }

    void enqueue(const T& item) {
        _queue.enqueue(item);

        uint64_t u = 1;
        if (::eventfd_write(_eventFd, u) < 0) {
            printf("eventfd_write goes wrong\n");
            exit(1);
        }
    }

    // TODO: Change the behavior according to the original version
    // note: this method will block until an item is available
    std::vector<T> dequeue() {
        uint64_t u {};
        if (::eventfd_read(_eventFd, &u) < 0) {
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                printf("eventfd_read goes wrong\n");
                exit(1);
            } else {
                return {};
            }
        }

        std::vector<T> vecT(u);
        for (auto i = 0uz; i < u; ++i) {
            // We have only single consumer, so this guarantees success or something BAD
            if (!_queue.try_dequeue(vecT[i])) {
                printf("Try dequeu goes wrong\n");
                exit(1);
            }
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

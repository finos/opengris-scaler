// #pragma once
//
// // System
// #include <sys/eventfd.h>
//
// // C++
// #include <memory>
// #include <optional>
//
// // First-party
// // #include "common.h"
// #include "event_loop_thread.h"
// #include "event_manager.h"
// #include "file_descriptor.h"
//
// // Third-party
// #include "scaler/io/ymq/event_manager.h"
// #include "third_party/concurrentqueue.h"
//
// using moodycamel::ConcurrentQueue;
//
// class EventManager;
// class EventLoopThread;
//
// template <typename T>
// class InterruptiveConcurrentQueue {
//     ConcurrentQueue<T> _queue;
//     FileDescriptor _eventFd;
//     std::unique_ptr<EventManager> _eventManager;
//
// public:
//     InterruptiveConcurrentQueue(EventLoopThread& thread, std::function<void(T)> callback): _queue() {
//         auto fd = FileDescriptor::eventfd(0, EFD_SEMAPHORE);
//
//         if (!fd) {
//             throw std::system_error(fd.error(), std::system_category(), "Failed to create eventfd");
//         }
//
//         _eventFd = std::move(*fd);
//
//         _eventManager = std::make_unique<EventManager>(
//             thread, std::move(_eventFd), [this, callback](FileDescriptor& fd, Events events) {
//                 if (events.readable) {
//                     // Signal that an item is available
//                     T item;
//                     this->dequeue(item);
//                     callback(item);
//                 }
//             });
//     }
//
//     // unmovable, uncopyable
//     InterruptiveConcurrentQueue(const InterruptiveConcurrentQueue&)            = delete;
//     InterruptiveConcurrentQueue& operator=(const InterruptiveConcurrentQueue&) = delete;
//     InterruptiveConcurrentQueue(InterruptiveConcurrentQueue&&)                 = delete;
//     InterruptiveConcurrentQueue& operator=(InterruptiveConcurrentQueue&&)      = delete;
//
//     void addToEventLoop(EventLoopThread& eventLoopThread) {}
//
//     // returns a non-owned file descriptor
//     FileDescriptor eventFd() const { return _eventFd; }
//
//     void enqueue(const T& item) {
//         _queue.enqueue(item);
//         _eventFd.eventfd_signal();
//     }
//
//     // note: this method will block until an item is available
//     std::optional<Errno> dequeue(T& item) {
//         if (auto result = _eventFd.eventfd_wait(); !result) {
//             // If the eventfd wait failed, we return false
//             return result;
//         }
//
//         for (;;) {
//             if (_queue.try_dequeue(item)) {
//                 return std::nullopt;  // success
//             }
//         }
//     }
// };

#pragma once

// System
#include <sys/eventfd.h>

// C++
#include <cstdlib>
#include <memory>
#include <optional>

// First-party
// #include "common.h"

// Third-party
#include "third_party/concurrentqueue.h"

using moodycamel::ConcurrentQueue;

class EventManager;

template <typename T>
class InterruptiveConcurrentQueue {
    int _eventFd;
    ConcurrentQueue<T> _queue;

public:
    InterruptiveConcurrentQueue(): _queue() { _eventFd = eventfd(0, EFD_SEMAPHORE); }

    int eventFd() { return _eventFd; }

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
    void dequeue(T& item) {
        uint64_t u;
        if (::eventfd_read(_eventFd, &u) < 0) {
            printf("eventfd_read goes wrong\n");
            exit(1);
        }

        _queue.try_dequeue(item);
        // for (;;) {
        //     if (_queue.try_dequeue(item)) {
        //         return;
        //         // exit(1);
        //     }
        // }
    }

    // unmovable, uncopyable
    InterruptiveConcurrentQueue(const InterruptiveConcurrentQueue&)            = delete;
    InterruptiveConcurrentQueue& operator=(const InterruptiveConcurrentQueue&) = delete;
    InterruptiveConcurrentQueue(InterruptiveConcurrentQueue&&)                 = delete;
    InterruptiveConcurrentQueue& operator=(InterruptiveConcurrentQueue&&)      = delete;
};

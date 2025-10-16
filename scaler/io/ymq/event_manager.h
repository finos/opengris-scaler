#pragma once

// C++
#ifdef __linux__
#include <sys/epoll.h>
#endif  // __linux__
#ifdef _WIN32
#include <windows.h>
#endif  // _WIN32
#ifdef __APPLE__
#include <sys/event.h>
#endif  // __APPLE__

#include <concepts>
#include <cstdint>  // uint64_t
#include <functional>

// First-party
#include "scaler/io/ymq/configuration.h"

namespace scaler {
namespace ymq {

#ifdef __APPLE__
constexpr uint64_t KQUEUE_EVENT_READ  = 1ULL << 0;
constexpr uint64_t KQUEUE_EVENT_WRITE = 1ULL << 1;
constexpr uint64_t KQUEUE_EVENT_CLOSE = 1ULL << 2;
constexpr uint64_t KQUEUE_EVENT_ERROR = 1ULL << 3;
#endif  // __APPLE__

class EventLoopThread;

// TODO: Add the _fd back
#if defined(_WIN32)
class EventManager: public OVERLAPPED {
#elif defined(__linux__) || defined(__APPLE__)
class EventManager {
#endif
    public:
        void onEvents(uint64_t events)
        {
#ifdef __linux__
            if constexpr (std::same_as<Configuration::PollingContext, EpollContext>) {
                int realEvents = (int)events;
                if ((realEvents & EPOLLHUP) && !(realEvents & EPOLLIN)) {
                    onClose();
                }
                if (realEvents & (EPOLLERR | EPOLLHUP)) {
                    onError();
                }
                if (realEvents & (EPOLLIN | EPOLLRDHUP)) {
                    onRead();
                }
                if (realEvents & EPOLLOUT) {
                    onWrite();
                }
            }
#endif  // __linux__
#ifdef _WIN32
            if constexpr (std::same_as<Configuration::PollingContext, IocpContext>) {
                onRead();
                onWrite();
                if (events & IOCP_SOCKET_CLOSED) {
                    onClose();
                }
            }
#endif  // _WIN32
#ifdef __APPLE__
            if constexpr (std::same_as<Configuration::PollingContext, KqueueContext>) {
                if (events & KQUEUE_EVENT_CLOSE) {
                    onClose();
                }
                if (events & KQUEUE_EVENT_ERROR) {
                    onError();
                }
                if (events & KQUEUE_EVENT_READ) {
                    onRead();
                }
                if (events & KQUEUE_EVENT_WRITE) {
                    onWrite();
                }
            }
#endif  // __APPLE__
        }

        // User that registered them should have everything they need
        // In the future, we might add more onXX() methods, for now these are all we need.
        using OnEventCallback = std::function<void()>;
        OnEventCallback onRead;
        OnEventCallback onWrite;
        OnEventCallback onClose;
        OnEventCallback onError;
        // EventManager(): _fd {} {}
        EventManager()
        {
#ifdef _WIN32
            ZeroMemory(this, sizeof(*this));
#endif  // _WIN32
        };
    };

}  // namespace ymq
}  // namespace scaler

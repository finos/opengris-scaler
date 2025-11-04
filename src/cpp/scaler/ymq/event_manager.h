#pragma once

#include <concepts>
#include <cstdint>  // uint64_t
#include <functional>

// First-party
#include "scaler/ymq/configuration.h"

namespace scaler {
namespace ymq {

class EventLoopThread;

#ifdef _WIN32
#define INHERIT_OVERLAPPED : public OVERLAPPED
#define ZERO_OVERLAPPED()                     \
    do {                                      \
        *static_cast<OVERLAPPED*>(this) = {}; \
    } while (0)
#else
#define INHERIT_OVERLAPPED
#define ZERO_OVERLAPPED()
#endif

// TODO: Add the _fd back
class EventManager INHERIT_OVERLAPPED {
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
    }

    // User that registered them should have everything they need
    // In the future, we might add more onXX() methods, for now these are all we need.
    using OnEventCallback = std::function<void()>;
    OnEventCallback onRead;
    OnEventCallback onWrite;
    OnEventCallback onClose;
    OnEventCallback onError;
    // EventManager(): _fd {} {}
    EventManager() { ZERO_OVERLAPPED(); };
};

}  // namespace ymq
}  // namespace scaler

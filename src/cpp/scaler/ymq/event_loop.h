#pragma once

// C++
#include <concepts>
#include <cstdint>  // uint64_t
#include <functional>

// First-party
#include "scaler/ymq/configuration.h"
#include "scaler/ymq/epoll_context.h"
#include "scaler/ymq/iocp_context.h"

namespace scaler {
namespace ymq {

struct Timestamp;
class EventManager;

template <class Backend>
concept EventLoopBackend = requires(Backend backend, Backend::Function f) {
    { backend.executeNow(std::move(f)) } -> std::same_as<void>;
    { backend.executeLater(std::move(f)) } -> std::same_as<void>;
    { backend.executeAt(Timestamp {}, std::move(f)) } -> std::integral;
    { backend.cancelExecution(0) } -> std::same_as<void>;

    backend.addFdToLoop(int {}, uint64_t {}, (EventManager*)nullptr);
    { backend.removeFdFromLoop(int {}) } -> std::same_as<void>;
};

template <EventLoopBackend Backend = EpollContext>
class EventLoop {
    Backend backend;

public:
    using Function   = Backend::Function;
    using Identifier = Backend::Identifier;

    void loop() { backend.loop(); }

    void executeNow(Function func) { backend.executeNow(std::move(func)); }
    void executeLater(Function func) { backend.executeLater(std::move(func)); }

    Identifier executeAt(Timestamp timestamp, Function func) { return backend.executeAt(timestamp, std::move(func)); }
    void cancelExecution(Identifier identifier) { backend.cancelExecution(identifier); }

    auto addFdToLoop(int fd, uint64_t events, EventManager* manager)
    {
        return backend.addFdToLoop(fd, events, manager);
    }

    void removeFdFromLoop(int fd) { backend.removeFdFromLoop(fd); }
};

}  // namespace ymq
}  // namespace scaler

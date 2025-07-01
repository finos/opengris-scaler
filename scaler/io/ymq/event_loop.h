#pragma once

// C++
#include <concepts>
#include <cstdint>  // uint64_t
#include <functional>

// First-party
#include "scaler/io/ymq/configuration.h"
#include "scaler/io/ymq/epoll_context.h"

struct Timestamp;
class EventManager;

template <class T>
concept EventLoopBackend = requires(T t, std::function<void()> f) {
    { t.executeNow(f) } -> std::same_as<void>;
    { t.executeLater(f) } -> std::same_as<void>;
    { t.executeAt(Timestamp {}, f) } -> std::integral;
    { t.cancelExecution(0) } -> std::same_as<void>;

    t.addFdToLoop(int {}, uint64_t {}, (EventManager*)nullptr);
    { t.removeFdFromLoop(int {}) } -> std::same_as<void>;
};

template <EventLoopBackend Backend = EpollContext>
class EventLoop {
    Backend backend;

public:
    using Function   = std::function<void()>;
    using Identifier = Configuration::ExecutionCancellationIdentifier;
    void loop() { backend.loop(); }

    void executeNow(Function func) { backend.executeNow(std::move(func)); }
    void executeLater(Function func) { backend.executeLater(std::move(func)); }

    Identifier executeAt(Timestamp timestamp, Function func) { return backend.executeAt(timestamp, std::move(func)); }
    void cancelExecution(Identifier identifier) { backend.cancelExecution(identifier); }

    // NOTE: These two functions are not used. - gxu
    void registerCallbackBeforeLoop(EventManager*);
    void registerEventManager(EventManager& em) { backend.registerEventManager(em); }

    auto addFdToLoop(int fd, uint64_t events, EventManager* manager) {
        return backend.addFdToLoop(fd, events, manager);
    }

    void removeFdFromLoop(int fd) { backend.removeFdFromLoop(fd); }
};

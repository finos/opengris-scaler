#pragma once

#include <expected>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <thread>

#include "scaler/utility/move_only_function.h"
#include "scaler/wrapper/uv/async.h"
#include "scaler/wrapper/uv/loop.h"

namespace scaler {
namespace uv_ymq {

// A thread running its own libuv event loop.
class EventLoopThread {
public:
    using Callback = scaler::utility::MoveOnlyFunction<void()>;

    EventLoopThread() noexcept;
    ~EventLoopThread() noexcept;

    EventLoopThread(EventLoopThread&&) noexcept            = default;
    EventLoopThread& operator=(EventLoopThread&&) noexcept = default;

    EventLoopThread(const EventLoopThread&) noexcept            = delete;
    EventLoopThread& operator=(const EventLoopThread&) noexcept = delete;

    scaler::wrapper::uv::Loop& loop() noexcept;

    // Schedule the execution of a function within the event loop thread.
    //
    // Thread-safe.
    void executeThreadSafe(Callback callback) noexcept;

private:
    scaler::wrapper::uv::Loop _loop;

    std::jthread _thread;

    // executeThreadSafe() add callbacks to a thread-safe queue, and then wake up the the UV loop using an uv::Async
    // notification.

    std::mutex _executeMutex;
    std::queue<Callback> _executeQueue;
    scaler::wrapper::uv::Async _executeAsync;

    void run() noexcept;

    void processExecuteCallbacks() noexcept;
};

}  // namespace uv_ymq
}  // namespace scaler

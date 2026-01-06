#pragma once

#include <expected>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <thread>

#include "scaler/utility/move_only_function.h"
#include "scaler/uv/async.h"
#include "scaler/uv/loop.h"

namespace scaler {
namespace uv_ymq {

// A thread running its own libuv event loop.
class UVLoopThread: public std::enable_shared_from_this<UVLoopThread> {
public:
    using Callback = utility::MoveOnlyFunction<void()>;

    UVLoopThread() noexcept;
    ~UVLoopThread() noexcept;

    UVLoopThread(UVLoopThread&&) noexcept            = default;
    UVLoopThread& operator=(UVLoopThread&&) noexcept = default;

    UVLoopThread(const UVLoopThread&) noexcept            = delete;
    UVLoopThread& operator=(const UVLoopThread&) noexcept = delete;

    uv::Loop& loop() noexcept;

    // Schedule the execution of a function within the event loop thread.
    //
    // Thread-safe.
    void execute(Callback&& callback) noexcept;

private:
    uv::Loop _loop;

    std::jthread _thread;

    // execute() add callbacks to a thread-safe queue, and then wake up the the UV loop using an uv::Async notification.

    std::mutex _executeMutex;
    std::queue<Callback> _executeQueue;
    uv::Async _executeAsync;

    void run() noexcept;

    void processExecuteCallbacks() noexcept;
};

}  // namespace uv_ymq
}  // namespace scaler

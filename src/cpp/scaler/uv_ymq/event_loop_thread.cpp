#include "scaler/uv_ymq/event_loop_thread.h"

#include <functional>
#include <utility>

#include "scaler/wrapper/uv/error.h"

namespace scaler {
namespace uv_ymq {

EventLoopThread::EventLoopThread() noexcept
    : _loop(UV_EXIT_ON_ERROR(scaler::wrapper::uv::Loop::init()))
    , _executeAsync(UV_EXIT_ON_ERROR(
          scaler::wrapper::uv::Async::init(_loop, std::bind_front(&EventLoopThread::processExecuteCallbacks, this))))
{
    _thread = std::jthread([this](std::stop_token stop_token) { run(); });
}

EventLoopThread::~EventLoopThread() noexcept
{
    assert(_thread.joinable());

    executeThreadSafe([this]() { _loop.stop(); });  // _loop.stop() must be called from the loop's thread.

    _thread.join();
}
scaler::wrapper::uv::Loop& EventLoopThread::loop() noexcept
{
    return _loop;
}

void EventLoopThread::executeThreadSafe(Callback callback) noexcept
{
    {
        std::lock_guard<std::mutex> lock(_executeMutex);
        _executeQueue.push(std::move(callback));
    }

    // Wake up the event loop
    UV_EXIT_ON_ERROR(_executeAsync.send());
}

void EventLoopThread::run() noexcept
{
    _loop.run(UV_RUN_DEFAULT);
}

void EventLoopThread::processExecuteCallbacks() noexcept
{
    std::queue<Callback> callbacks;
    {
        std::lock_guard<std::mutex> lock(_executeMutex);
        std::swap(callbacks, _executeQueue);
    }

    // Process all callbacks outside the lock
    while (!callbacks.empty()) {
        callbacks.front()();
        callbacks.pop();
    }
}

}  // namespace uv_ymq
}  // namespace scaler

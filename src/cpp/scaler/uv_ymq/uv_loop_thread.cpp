#include "scaler/uv_ymq/uv_loop_thread.h"

#include <functional>
#include <utility>

#include "scaler/uv/error.h"

namespace scaler {
namespace uv_ymq {

UVLoopThread::UVLoopThread() noexcept
    : _loop(UV_EXIT_ON_ERROR(uv::Loop::init()))
    , _executeAsync(
          UV_EXIT_ON_ERROR(uv::Async::init(_loop, std::bind_front(&UVLoopThread::processExecuteCallbacks, this))))
{
    _thread = std::jthread([this](std::stop_token stop_token) { run(); });
}

UVLoopThread::~UVLoopThread() noexcept
{
    assert(_thread.joinable());

    execute([this]() { _loop.stop(); });  // _loop.stop() must be called from the loop's thread.
}
uv::Loop& UVLoopThread::loop() noexcept
{
    return _loop;
}

void UVLoopThread::execute(Callback&& callback) noexcept
{
    {
        std::lock_guard<std::mutex> lock(_executeMutex);
        _executeQueue.push(std::move(callback));
    }

    // Wake up the event loop
    UV_EXIT_ON_ERROR(_executeAsync.send());
}

void UVLoopThread::run() noexcept
{
    _loop.run(UV_RUN_DEFAULT);
}

void UVLoopThread::processExecuteCallbacks() noexcept
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

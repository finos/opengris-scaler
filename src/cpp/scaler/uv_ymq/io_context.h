#pragma once

#include <atomic>
#include <memory>
#include <vector>

#include "scaler/uv_ymq/event_loop_thread.h"

namespace scaler {
namespace uv_ymq {

// Manages a pool of IO event threads.
class IOContext {
public:
    IOContext(size_t threadCount = 1) noexcept;

    ~IOContext() noexcept = default;

    IOContext(const IOContext&)            = delete;
    IOContext& operator=(const IOContext&) = delete;

    IOContext(IOContext&&)            = default;
    IOContext& operator=(IOContext&&) = default;

    // Fetch the next thread in the round-robin pool.
    EventLoopThread& nextThread() noexcept;

    constexpr size_t numThreads() const noexcept { return _threads.size(); }

private:
    std::vector<EventLoopThread> _threads;
    std::atomic<size_t> _threadsRoundRobin;
};

}  // namespace uv_ymq
}  // namespace scaler

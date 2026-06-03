#include "scaler/ymq/io_context.h"

#include <cassert>

namespace scaler {
namespace ymq {

IOContext::IOContext(size_t threadCount) noexcept
    : _threads(threadCount), _threadsRoundRobin {std::make_unique<std::atomic<size_t>>(0)}
{
    assert(threadCount > 0);
}

internal::EventLoopThread& IOContext::nextThread() noexcept
{
    size_t index = _threadsRoundRobin->fetch_add(1) % _threads.size();
    return _threads[index];
}

}  // namespace ymq
}  // namespace scaler

#include "scaler/uv_ymq/io_context.h"

#include <algorithm>
#include <cassert>

namespace scaler {
namespace uv_ymq {

IOContext::IOContext(size_t threadCount) noexcept: _threads(threadCount), _threadsRoundRobin {0}
{
    assert(threadCount > 0);
}

EventLoopThread& IOContext::nextThread() noexcept
{
    auto& thread = _threads[_threadsRoundRobin];
    ++_threadsRoundRobin;
    _threadsRoundRobin = _threadsRoundRobin % _threads.size();
    return thread;
}

}  // namespace uv_ymq
}  // namespace scaler
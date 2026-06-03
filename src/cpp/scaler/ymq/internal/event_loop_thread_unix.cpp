#include <pthread.h>

#include <csignal>

#include "scaler/ymq/internal/event_loop_thread.h"

namespace scaler {
namespace ymq {
namespace internal {

void EventLoopThread::initialize() noexcept
{
    // Ignore SIGPIPE events on the IO thread. These will ultimately be handled by YMQ, when writing or reading
    // messages.
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGPIPE);

    pthread_sigmask(SIG_BLOCK, &set, nullptr);
}

}  // namespace internal
}  // namespace ymq
}  // namespace scaler

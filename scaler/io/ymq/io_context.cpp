#include "scaler/io/ymq/event_loop_thread.h"
#include "scaler/io/ymq/io_context.h"

IOSocket* IOContext::addIOSocket(std::string identity, std::string socketType) {
    std::lock_guard guard {_threadsMu};
    static size_t threadsHead {0};
    auto res = _threads[threadsHead].addIOSocket(identity, socketType);
    ++threadsHead %= _threads.size();
    return res;
}

bool IOContext::removeIOSocket(IOSocket*) {
    return false;
}


#include "scaler/io/ymq/io_context.h"

#include <algorithm>  // std::ranges::generate
#include <cassert>    // assert

#include "scaler/io/ymq/event_loop_thread.h"
#include "scaler/io/ymq/io_socket.h"

IOContext::IOContext(size_t threadCount): _threads(threadCount) {
    assert(threadCount > 0);
    std::ranges::generate(_threads, std::make_shared<EventLoopThread>);
}

IOSocket* IOContext::createIOSocket(std::string identity, IOSocketType socketType) {
    std::lock_guard guard {_threadsMu};
    static size_t threadsHead {0};
    auto res = _threads[threadsHead]->createIOSocket(identity, socketType);
    ++threadsHead %= _threads.size();
    return res;
}

bool IOContext::removeIOSocket(IOSocket*) {
    return false;
}

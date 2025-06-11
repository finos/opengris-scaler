
#include "scaler/io/ymq/event_loop_thread.h"

#include <cassert>

#include "scaler/io/ymq/io_socket.h"

IOSocket* EventLoopThread::createIOSocket(std::string identity, IOSocketType socketType) {
    if (thread.get_id() == std::thread::id()) {
        thread = std::jthread([this](std::stop_token token) {
            while (!token.stop_requested()) {
                this->eventLoop.loop();
            }
        });
    }

    auto [iterator, inserted] = identityToIOSocket.try_emplace(identity, shared_from_this(), identity, socketType);
    assert(inserted);
    auto ptr = &iterator->second;

    // TODO: Something happen with the running thread
    eventLoop.executeNow([ptr] { ptr->onCreated(); });
    return ptr;
}

// TODO: Think about non null pointer
void EventLoopThread::removeIOSocket(IOSocket* target) {
    // TODO: Something happen with the running thread
    identityToIOSocket.erase(target->identity());
}

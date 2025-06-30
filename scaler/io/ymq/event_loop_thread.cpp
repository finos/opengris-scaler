
#include "scaler/io/ymq/event_loop_thread.h"

#include <cassert>
#include <memory>

#include "scaler/io/ymq/event_manager.h"
#include "scaler/io/ymq/io_socket.h"

std::shared_ptr<IOSocket> EventLoopThread::createIOSocket(
    std::string identity, IOSocketType socketType, std::function<void()> callback) {
    if (thread.get_id() == std::thread::id()) {
        thread = std::jthread([this](std::stop_token token) {
            while (!token.stop_requested()) {
                this->_eventLoop.loop();
            }
        });
    }

    auto [iterator, inserted] =
        _identityToIOSocket.try_emplace(identity, std::make_shared<IOSocket>(shared_from_this(), identity, socketType));
    assert(inserted);
    auto ptr = iterator->second;

    _eventLoop.executeNow([ptr, callback = std::move(callback)] { callback(); });

    return ptr;
}

void EventLoopThread::removeIOSocket(IOSocket* target) {
    assert(_identityToIOSocket[target->identity()].use_count() == 1);
    _identityToIOSocket.erase(target->identity());
}

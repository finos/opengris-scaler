
#include "scaler/io/ymq/event_loop_thread.h"

#include <cassert>
#include <memory>

#include "scaler/io/ymq/event_manager.h"
#include "scaler/io/ymq/io_socket.h"

namespace scaler {
namespace ymq {

void EventLoopThread::createIOSocket(std::string identity, IOSocketType socketType, CreateIOSocketCallback callback) {
    if (thread.get_id() == std::thread::id()) {
        thread = std::jthread([this](std::stop_token token) {
            while (!token.stop_requested()) {
                this->_eventLoop.loop();
            }
        });
    }

    _eventLoop.executeNow([this, callback = std::move(callback), identity = std::move(identity), socketType] mutable {
        auto [iterator, inserted] = _identityToIOSocket.try_emplace(
            identity, std::make_shared<IOSocket>(shared_from_this(), identity, socketType));
        assert(inserted);
        auto ptr = iterator->second;

        callback(ptr);
    });
}

void EventLoopThread::removeIOSocket(IOSocket* target) {
    assert(_identityToIOSocket[target->identity()].use_count() == 1);
    _identityToIOSocket.erase(target->identity());
    if (_identityToIOSocket.empty()) {
        thread.request_stop();
    }
}

}  // namespace ymq
}  // namespace scaler

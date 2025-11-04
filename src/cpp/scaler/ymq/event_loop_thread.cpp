
#include "scaler/ymq/event_loop_thread.h"

#include <cassert>
#include <memory>

#include "scaler/error/error.h"
#include "scaler/ymq/event_manager.h"
#include "scaler/ymq/io_socket.h"

namespace scaler {
namespace ymq {

void EventLoopThread::createIOSocket(std::string identity, IOSocketType socketType, CreateIOSocketCallback callback)
{
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

        if (!inserted) {
            unrecoverableError({
                Error::ErrorCode::RepetetiveIOSocketIdentity,
                "Originated from (in EventLoopThread::createIOSocket)",
                __PRETTY_FUNCTION__,
                "Your input identity",
                identity,
            });
        }

        auto ptr = iterator->second;

        callback(ptr);
    });
}

void EventLoopThread::removeIOSocket(IOSocket* target)
{
    auto useCount = _identityToIOSocket[target->identity()].use_count();
    if (useCount != 1) {
        unrecoverableError({
            Error::ErrorCode::RedundantIOSocketRefCount,
            "Originated from",
            __PRETTY_FUNCTION__,
            "use_count",
            useCount,
        });
    }

    _identityToIOSocket.erase(target->identity());
    if (_identityToIOSocket.empty()) {
        thread.request_stop();
    }
}

}  // namespace ymq
}  // namespace scaler

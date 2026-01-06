
#include "scaler/ymq/io_context.h"

#include <algorithm>  // std::ranges::generate
#include <cassert>    // assert
#include <future>
#include <memory>  // std::make_shared
#ifdef _WIN32
// clang-format off
#include <winsock2.h>
#include <mswsock.h>
// clang-format on
#endif  // _WIN32

#include "scaler/ymq/event_loop_thread.h"
#include "scaler/ymq/io_socket.h"
#include "scaler/ymq/typedefs.h"

namespace scaler {
namespace ymq {

IOContext::IOContext(size_t threadCount) noexcept: _threads(threadCount), _threadsRoundRobin {}
{
    assert(threadCount > 0);
    std::ranges::generate(_threads, std::make_shared<EventLoopThread>);
#ifdef _WIN32
    WSADATA wsaData;
    const int myErrno = WSAStartup(MAKEWORD(2, 2), &wsaData);
    if (myErrno != 0) {
        unrecoverableError({
            Error::ErrorCode::ConfigurationError,
            "Originated from",
            "WSAStartup",
            "Errno is",
            strerror(myErrno),
        });
    }
#endif  // _WIN32
}

void IOContext::createIOSocket(
    Identity identity, IOSocketType socketType, CreateIOSocketCallback onIOSocketCreated) & noexcept
{
    auto& thread = _threads[_threadsRoundRobin];
    ++_threadsRoundRobin;
    _threadsRoundRobin = _threadsRoundRobin % _threads.size();
    thread->createIOSocket(std::move(identity), socketType, std::move(onIOSocketCreated));
}

void IOContext::removeIOSocket(std::shared_ptr<IOSocket>& socket) noexcept
{
    auto* rawSocket = socket.get();
    socket.reset();

    // FIXME: This is a tmp fix in order to keep the changes small.
    // A better fix would be to put "id" in each shared_ptr<EventLoopThread> and query that
    // id to get the index. This is needed so we shutdown the thread when it's not needed.
    // The better fix should be implemented in the next PR.
    int id = -1;
    {
        // NOTE: Keep the eventloop thread alive
        auto eventLoopThread = rawSocket->_eventLoopThread;
        rawSocket->_eventLoopThread->_eventLoop.executeNow([=] { rawSocket->requestStop(); });
        std::promise<void> promise;
        auto future = promise.get_future();

        // TODO: This `count` and `maxCount` is needed as a safety net so that
        // we don't wait forever on querying numOfConnections.
        // If the remote end is using YMQ as internal communication tool, then
        // we don't need this safety net. This is because YMQ closes a connection
        // if the remote end shutdown write. This results to an event in local.
        // If the remote end does not close connection upon local end shutdown
        // write, the local end will never get any event for remote socket close,
        // and therefore the connection will stay alive in the system.
        // This needs to be revisited, we have opened an issue, the issue link is:
        // https://github.com/finos/opengris-scaler/issues/445
        int count                 = 0;
        auto waitToRemoveIOSocket = [&](const auto& self) -> void {
            constexpr static int maxCount = 8;
            rawSocket->_eventLoopThread->_eventLoop.executeNow([&] {
                rawSocket->_eventLoopThread->_eventLoop.executeLater([&] {
                    if (rawSocket->numOfConnections() && count < maxCount) {
                        ++count;
                        self(self);
                        return;
                    }
                    rawSocket->_eventLoopThread->removeIOSocket(rawSocket);
                    promise.set_value();
                });
            });
        };
        waitToRemoveIOSocket(waitToRemoveIOSocket);

        future.wait();

        if (eventLoopThread->stopRequested()) {
            auto it = std::ranges::find_if(_threads, [&](const auto& x) { return x.get() == eventLoopThread.get(); });
            id      = std::distance(_threads.begin(), it);
        }
    }
    if (id != -1) {
        _threads[id]->tryJoin();
        _threads[id] = std::make_shared<EventLoopThread>();
    }
}

void IOContext::requestIOSocketStop(std::shared_ptr<IOSocket> socket) noexcept
{
    socket->_eventLoopThread->_eventLoop.executeNow([socket] { socket->requestStop(); });
}

IOContext::~IOContext() noexcept
{
#ifdef _WIN32
    WSACleanup();
#endif  // _WIN32
}

}  // namespace ymq
}  // namespace scaler

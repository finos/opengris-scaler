
#include "scaler/io/ymq/io_context.h"

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

#include "scaler/io/ymq/event_loop_thread.h"
#include "scaler/io/ymq/io_socket.h"
#include "scaler/io/ymq/typedefs.h"

namespace scaler {
namespace ymq {

IOContext::IOContext(size_t threadCount) noexcept: _threads(threadCount)
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
    static std::atomic<size_t> threadsRoundRobin = 0;
    auto& thread                                 = _threads[threadsRoundRobin];
    ++threadsRoundRobin;
    threadsRoundRobin = threadsRoundRobin % _threads.size();
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
        std::promise<void> promise;
        auto future = promise.get_future();
        rawSocket->_eventLoopThread->_eventLoop.executeNow([&promise, rawSocket] {
            rawSocket->_eventLoopThread->_eventLoop.executeLater([&promise, rawSocket] {
                rawSocket->_eventLoopThread->removeIOSocket(rawSocket);
                promise.set_value();
            });
        });
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

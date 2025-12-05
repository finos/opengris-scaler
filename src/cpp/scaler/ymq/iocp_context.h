#pragma once
#ifdef _WIN32


// C++
#include <functional>
#include <queue>

#include "scaler/ymq/configuration.h"
#include "scaler/ymq/timed_queue.h"

// First-party
#include "scaler/ymq/interruptive_concurrent_queue.h"
#include "scaler/ymq/timestamp.h"
// clang-format off
#define NOMINMAX
#include <windows.h>
#include <winsock2.h>
#include <mswsock.h>
#include <ws2tcpip.h> // inet_pton
// clang-format on

#undef SendMessageCallback
#define __PRETTY_FUNCTION__ __FUNCSIG__
#define EPOLLIN             (0)
#define EPOLLOUT            (0)
#define EPOLLET             (0)


namespace scaler {
namespace ymq {

class EventManager;

// In the constructor, the epoll context should register eventfd/timerfd from
// This way, the queues need not know about the event manager. We don't use callbacks.
class IOCPContext {
public:
    using Function             = Configuration::ExecutionFunction;
    using DelayedFunctionQueue = std::queue<Function>;
    using Identifier           = Configuration::ExecutionCancellationIdentifier;
    HANDLE _completionPort;

    // TODO: Handle error with unrecoverable error in the next PR.
    IOCPContext()
        : _completionPort(CreateIoCompletionPort(INVALID_HANDLE_VALUE, nullptr, (ULONG_PTR)0, 1))
        , _timingFunctions(_completionPort, _isTimingFd)
        , _interruptiveFunctions(_completionPort, _isInterruptiveFd)
    {
        if (!_completionPort) {
            unrecoverableError({
                Error::ErrorCode::CoreBug,
                "Originated from",
                "CreateIoCompletionPort",
                "Errno is",
                GetLastError(),
            });
        }
    }

    ~IOCPContext() { CloseHandle(_completionPort); }

    void loop();

    void addFdToLoop(int fd, uint64_t, EventManager*);
    void removeFdFromLoop(int fd);

    // NOTE: Thread-safe method to communicate with the event loop thread
    void executeNow(Function func) { _interruptiveFunctions.enqueue(std::move(func)); }
    // WARN: NOT thread-safe. Thread safety is guaranteed by executeNow.
    void executeLater(Function func) { _delayedFunctions.emplace(std::move(func)); }
    // WARN: NOT thread-safe. Thread safety is guaranteed by executeNow.
    Identifier executeAt(Timestamp timestamp, Function callback)
    {
        return _timingFunctions.push(timestamp, std::move(callback));
    }
    void cancelExecution(Identifier identifier) { _timingFunctions.cancelExecution(identifier); }

private:
    std::set<int> _sockets;
    void execPendingFunctions();
    TimedQueue _timingFunctions;
    DelayedFunctionQueue _delayedFunctions;
    InterruptiveConcurrentQueue<Function> _interruptiveFunctions;
    constexpr static const size_t _isInterruptiveFd = 0;
    constexpr static const size_t _isTimingFd       = 1;
    constexpr static const size_t _isSocket         = 2;
    constexpr static const size_t _reventSize       = 128;  // Reduced for linter.
};

}  // namespace ymq
}  // namespace scaler

#endif  // _WIN32

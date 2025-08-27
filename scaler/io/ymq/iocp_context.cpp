#ifdef _WIN32

#include "scaler/io/ymq/iocp_context.h"

#include <cerrno>
#include <functional>

#include "scaler/io/ymq/error.h"
#include "scaler/io/ymq/event_manager.h"

namespace scaler {
namespace ymq {

void IocpContext::execPendingFunctions()
{
    while (_delayedFunctions.size()) {
        auto top = std::move(_delayedFunctions.front());
        top();
        _delayedFunctions.pop();
    }
}

void IocpContext::loop()
{
    std::array<OVERLAPPED_ENTRY, _reventSize> events {};
    ULONG n         = 0;
    const bool res  = GetQueuedCompletionStatusEx(_completionPort, events.data(), _reventSize, &n, INFINITE, true);
    uint64_t revent = 0;
    if (!res) {
        int lastError = GetLastError();
        if (lastError == WAIT_IO_COMPLETION) {
            auto vec = _timingFunctions.dequeue();
            std::ranges::for_each(vec, [](auto& x) { x(); });
            return;
        }

        if (lastError == ERROR_ABANDONED_WAIT_0) {
            revent |= IOCP_SOCKET_CLOSED;
        } else {
            fprintf(stderr, "GetQueuedCompletionStatusEx failed with error %d\n", lastError);
            exit(1);
        }
    }

    // NOTE: Timer events are handled above
    for (auto it = events.begin(); it != events.begin() + n; ++it) {
        auto current_event = *it;
        if (current_event.lpCompletionKey == _isInterruptiveFd) {
            auto vec = _interruptiveFunctions.dequeue();
            std::ranges::for_each(vec, [](auto&& x) { x(); });
            continue;
        }
        if (current_event.lpCompletionKey == _isSocket) {
            auto event = (EventManager*)(current_event.lpOverlapped);
            // TODO: Figure out whether there is a better way to remove overlapped entry from the IOCP queue
            if (!event) {
                continue;
            }
            // TODO: Figure out the best stuff to put in
            event->onEvents(revent);
        }
    }
    execPendingFunctions();
}

void IocpContext::addFdToLoop(int fd, uint64_t, EventManager*)
{
    const DWORD threadCount = 1;
    if (!CreateIoCompletionPort((HANDLE)(SOCKET)fd, _completionPort, _isSocket, threadCount)) {
        const int res = GetLastError();
        // NOTE: This is when the same fd being added to the loop more than once, normal.
        if (res == ERROR_INVALID_PARAMETER) {
            return;
        }

        // TODO: Better error handling
        fprintf(stderr, "addFdToLoop CreateIOCompletionPort res = %d, exit\n", res);
        exit(1);
    }
}

// NOTE: IOCP is based on single action instead of the file handle.
//  The file handle is automaticaly released when one call closesocket(fd).
//  This interface is required by the concept, and we need it for select(2) or poll(2).
//  Instead of relaxing constraint, we leave the implementation empty.
void IocpContext::removeFdFromLoop(int fd)
{
}

}  // namespace ymq
}  // namespace scaler

#endif  // _WIN32

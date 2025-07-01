#pragma once

#include <sys/timerfd.h>
#include <unistd.h>

#include <cassert>
#include <functional>
#include <queue>
#include <ranges>

#include "scaler/io/ymq/configuration.h"
#include "scaler/io/ymq/timestamp.h"

inline int createTimerfd() {
    int timerfd = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    if (timerfd < 0) {
        exit(1);
    }
    return timerfd;
}

// TODO: HANDLE ERRS
class TimedQueue {
public:
    using Callback   = Configuration::TimedQueueCallback;
    using Identifier = Configuration::ExecutionCancellationIdentifier;
    using TimedFunc  = std::tuple<Timestamp, Callback, Identifier>;

    TimedQueue(): _timerFd(createTimerfd()), _currentId {} { assert(_timerFd); }
    ~TimedQueue() {
        if (_timerFd > 0)
            close(_timerFd);
    }

    Identifier push(Timestamp timestamp, Callback cb) {
        auto ts = convertToItimerspec(timestamp);
        if (pq.empty() || timestamp < std::get<0>(pq.top())) {
            int ret = timerfd_settime(_timerFd, 0, &ts, nullptr);
            assert(ret == 0);
        }
        pq.push({timestamp, cb, _currentId});
        return _currentId++;
    }

    void cancelExecution(Identifier id) { _cancelledFunctions.push_back(id); }

    std::vector<Callback> dequeue() {
        uint64_t numItems;
        ssize_t n = read(_timerFd, &numItems, sizeof numItems);
        if (n != sizeof numItems) {
            assert(false);
            // Handle read error or spurious wakeup
            return {};
        }

        std::vector<Callback> callbacks;

        Timestamp now;
        while (pq.size()) {
            if (std::get<0>(pq.top()) < now) {
                auto [ts, cb, id] = std::move(pq.top());
                pq.pop();
                auto cancelled = std::ranges::find(_cancelledFunctions, id);
                if (cancelled != _cancelledFunctions.end()) {
                    std::erase(_cancelledFunctions, id);
                } else {
                    callbacks.push_back(std::move(cb));
                }
            } else
                break;
        }

        if (!pq.empty()) {
            auto nextTs = std::get<0>(pq.top());
            auto ts     = convertToItimerspec(nextTs);
            int ret     = timerfd_settime(_timerFd, 0, &ts, nullptr);
            if (ret == -1) {
                assert(false);
                // handle error
            }
        }
        return callbacks;
    }

    int timingFd() const { return _timerFd; }

private:
    int _timerFd;
    Identifier _currentId;
    constexpr static auto cmp = [](const auto& x, const auto& y) { return std::get<0>(x) < std::get<0>(y); };
    std::priority_queue<TimedFunc, std::vector<TimedFunc>, decltype(cmp)> pq;
    std::vector<Identifier> _cancelledFunctions;
};

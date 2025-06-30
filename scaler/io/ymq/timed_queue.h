#pragma once

#include <sys/timerfd.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <functional>
#include <queue>

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
    using Callback   = Configuration::TimedQueueCallback;
    using Identifier = Configuration::ExecutionCancellationIdentifier;

    using TimedFunc = std::tuple<Timestamp, Callback, Identifier>;
    using cmp       = decltype([](const auto& x, const auto& y) { return std::get<0>(x) < std::get<0>(y); });

    int timer_fd;
    Identifier _currentId;
    std::priority_queue<TimedFunc, std::vector<TimedFunc>, cmp> pq;
    std::vector<Identifier> _cancelledFunctions;

public:
    TimedQueue(): timer_fd(createTimerfd()), _currentId {} { assert(timer_fd); }

    Identifier push(Timestamp timestamp, Callback cb) {
        auto ts = convertToItimerspec(timestamp);
        if (pq.empty() || timestamp < std::get<0>(pq.top())) {
            int ret = timerfd_settime(timer_fd, 0, &ts, nullptr);
            assert(ret == 0);
        }
        pq.push({timestamp, cb, _currentId});
        return _currentId++;
    }

    void cancelExecution(Identifier id) { _cancelledFunctions.push_back(id); }

    void onRead() {
        uint64_t numItems;
        ssize_t n = read(timer_fd, &numItems, sizeof numItems);
        if (n != sizeof numItems) {
            assert(false);
            // Handle read error or spurious wakeup
            return;
        }

        Timestamp now;
        while (pq.size()) {
            if (std::get<0>(pq.top()) < now) {
                auto [ts, cb, id] = std::move(pq.top());
                pq.pop();
                auto cancelled = std::ranges::find(_cancelledFunctions, id);
                if (cancelled != _cancelledFunctions.end()) {
                    std::erase(_cancelledFunctions, id);
                } else {
                    cb();
                }
            } else
                break;
        }

        if (!pq.empty()) {
            auto nextTs = std::get<0>(pq.top());
            auto ts     = convertToItimerspec(nextTs);
            int ret     = timerfd_settime(timer_fd, 0, &ts, nullptr);
            if (ret == -1) {
                assert(false);
                // handle error
            }
        }
    }

    int timingFd() const { return timer_fd; }
};

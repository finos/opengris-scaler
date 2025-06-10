#pragma once

#include <sys/timerfd.h>
#include <unistd.h>

#include <functional>
#include <queue>

#include "scaler/io/ymq/timestamp.h"

inline int createTimerfd() {
    int timerfd = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    if (timerfd < 0) {
        exit(1);
    }
    return timerfd;
}

// TODO: HANDLE ERRS
struct TimedQueue {
    int timer_fd;
    using callback_t = std::function<void()>;
    using timed_fn   = std::pair<Timestamp, callback_t>;
    using cmp        = decltype([](const auto& x, const auto& y) { return x.first < y.first; });

    std::priority_queue<timed_fn, std::vector<timed_fn>, cmp> pq;

    TimedQueue(): timer_fd(createTimerfd()) {}

    void push(Timestamp timestamp, callback_t cb) {
        if (pq.size() && timestamp < pq.top().first) {
            auto ts = convertToItimerspec(timestamp);
            int ret = timerfd_settime(timer_fd, 0, &ts, nullptr);
        }

        pq.push({timestamp, cb});
    }

    void onRead() {
        uint64_t numItems;
        ssize_t n = read(timer_fd, &numItems, sizeof numItems);

        Timestamp now;
        while (pq.size()) {
            auto [ts, cb] = pq.top();
            if (ts < now) {
                cb();
                pq.pop();
            } else
                break;
        }
    }
};

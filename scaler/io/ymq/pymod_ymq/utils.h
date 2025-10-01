#pragma once

// Python
#include <optional>
#include <stdexcept>

#include "scaler/io/ymq/pymod_ymq/python.h"

// C++
#include <memory>

// C
#include <sys/eventfd.h>
#include <sys/poll.h>
#include <sys/timerfd.h>

#include <print>

// First-party
#include "scaler/io/ymq/common.h"
#include "scaler/io/ymq/pymod_ymq/ymq.h"

enum class WaitResult {
    Ok,
    Signal,
    Timeout,
};

class Waiter {
public:
    Waiter(int wakeFd, std::optional<int> timeout_secs = std::nullopt)
        : _timeout_secs(timeout_secs)
        , _timer_fd(std::shared_ptr<int>(new int, &destroy_fd))
        , _waiter(std::shared_ptr<int>(new int, &destroy_fd))
        , _wake_fd(wakeFd)
    {
        auto efd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
        if (efd < 0)
            throw std::runtime_error("failed to create eventfd");

        *_waiter = efd;

        auto tfd = timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC | TFD_NONBLOCK);
        if (tfd < 0)
            throw std::runtime_error("failed to create timerfd");

        *_timer_fd = tfd;
    }

    Waiter(const Waiter& other): _waiter(other._waiter), _wake_fd(other._wake_fd) {}
    Waiter(Waiter&& other) noexcept: _waiter(std::move(other._waiter)), _wake_fd(other._wake_fd)
    {
        other._wake_fd = -1;  // invalidate the moved-from object
    }

    Waiter& operator=(const Waiter& other)
    {
        if (this == &other)
            return *this;

        this->_waiter  = other._waiter;
        this->_wake_fd = other._wake_fd;
        return *this;
    }

    Waiter& operator=(Waiter&& other) noexcept
    {
        if (this == &other)
            return *this;

        this->_waiter  = std::move(other._waiter);
        this->_wake_fd = other._wake_fd;
        other._wake_fd = -1;  // invalidate the moved-from object
        return *this;
    }

    void signal()
    {
        if (eventfd_write(*_waiter, 1) < 0) {
            std::println(stderr, "Failed to signal waiter: {}", std::strerror(errno));
        }
    }

    WaitResult wait()
    {
        pollfd pfds[3] = {
            {
                .fd      = *_waiter,
                .events  = POLLIN,
                .revents = 0,
            },
            {
                .fd      = _wake_fd,
                .events  = POLLIN,
                .revents = 0,
            },
            {
                .fd      = *_timer_fd,
                .events  = POLLIN,
                .revents = 0,
            }};

        if (_timeout_secs) {
            itimerspec new_value {
                .it_interval = {0, 0},
                .it_value    = {*_timeout_secs, 0},
            };

            if (timerfd_settime(*_timer_fd, 0, &new_value, nullptr) < 0)
                throw std::runtime_error("failed to set timerfd");
        }

        for (;;) {
            int ready = poll(pfds, 3, -1);
            if (ready < 0) {
                if (errno == EINTR)
                    continue;
                throw std::runtime_error("poll failed");
            }

            if (pfds[0].revents & POLLIN)
                return WaitResult::Ok;  // we got a message

            if (pfds[1].revents & POLLIN)
                return WaitResult::Signal;  // signal received

            if (pfds[2].revents & POLLIN)
                return WaitResult::Timeout;  // timeout
        }
    }

private:
    std::optional<int> _timeout_secs;
    std::shared_ptr<int> _timer_fd;
    std::shared_ptr<int> _waiter;
    int _wake_fd;

    static void destroy_fd(int* fd)
    {
        if (!fd)
            return;

        close(*fd);
        delete fd;
    }
};

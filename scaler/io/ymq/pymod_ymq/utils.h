#pragma once

// Python
#include <stdexcept>

#include "scaler/io/ymq/pymod_ymq/python.h"

// C++
#include <memory>

// C
#include <sys/eventfd.h>
#include <sys/poll.h>

#include <print>

// First-party
#include "scaler/io/ymq/common.h"
#include "scaler/io/ymq/pymod_ymq/ymq.h"

class Waiter {
public:
    Waiter(int wakeFd): _waiter(std::shared_ptr<int>(new int, &destroy_efd)), _wakeFd(wakeFd)
    {
        auto fd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
        if (fd < 0)
            throw std::runtime_error("failed to create eventfd");

        *_waiter = fd;
    }

    Waiter(const Waiter& other): _waiter(other._waiter), _wakeFd(other._wakeFd) {}
    Waiter(Waiter&& other) noexcept: _waiter(std::move(other._waiter)), _wakeFd(other._wakeFd)
    {
        other._wakeFd = -1;  // invalidate the moved-from object
    }

    Waiter& operator=(const Waiter& other)
    {
        if (this == &other)
            return *this;

        this->_waiter = other._waiter;
        this->_wakeFd = other._wakeFd;
        return *this;
    }

    Waiter& operator=(Waiter&& other) noexcept
    {
        if (this == &other)
            return *this;

        this->_waiter = std::move(other._waiter);
        this->_wakeFd = other._wakeFd;
        other._wakeFd = -1;  // invalidate the moved-from object
        return *this;
    }

    void signal()
    {
        if (eventfd_write(*_waiter, 1) < 0) {
            std::println(stderr, "Failed to signal waiter: {}", std::strerror(errno));
        }
    }

    // true -> error
    // false -> ok
    bool wait()
    {
        pollfd pfds[2] = {
            {
                .fd      = *_waiter,
                .events  = POLLIN,
                .revents = 0,
            },
            {
                .fd      = _wakeFd,
                .events  = POLLIN,
                .revents = 0,
            }};

        for (;;) {
            int ready = poll(pfds, 2, -1);
            if (ready < 0) {
                if (errno == EINTR)
                    continue;
                throw std::runtime_error("poll failed");
            }

            if (pfds[0].revents & POLLIN)
                return false;  // we got a message

            if (pfds[1].revents & POLLIN)
                return true;  // signal received
        }
    }

private:
    std::shared_ptr<int> _waiter;
    int _wakeFd;

    static void destroy_efd(int* fd)
    {
        if (!fd)
            return;

        close(*fd);
        delete fd;
    }
};

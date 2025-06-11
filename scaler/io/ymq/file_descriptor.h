#pragma once

// System
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/timerfd.h>
#include <unistd.h>

// C
#include <cerrno>

// C++
#include <expected>
#include <optional>

// First-party
#include "scaler/io/ymq/common.h"

class FileDescriptor {
    int fd;

    FileDescriptor(int fd): fd(fd) {}

public:
    ~FileDescriptor() noexcept(false) {
        if (auto code = close(fd) < 0)
            throw std::system_error(errno, std::system_category(), "Failed to close file descriptor");

        this->fd = -1;
    }

    FileDescriptor(): fd(-1) {}

    // move-only
    FileDescriptor(const FileDescriptor&)            = delete;
    FileDescriptor& operator=(const FileDescriptor&) = delete;
    FileDescriptor(FileDescriptor&& other) noexcept: fd(other.fd) {
        other.fd = -1;  // prevent double close
    }
    FileDescriptor& operator=(FileDescriptor&& other) noexcept {
        if (this != &other) {
            if (fd >= 0) {
                close(fd);  // close current fd
            }
            fd       = other.fd;
            other.fd = -1;  // prevent double close
        }
        return *this;
    }

    static std::expected<FileDescriptor, Errno> socket(int domain, int type, int protocol) {
        if (int fd = ::socket(domain, type, protocol) < 0) {
            return std::unexpected {errno};
        } else {
            return FileDescriptor(fd);
        }
    }

    static std::expected<FileDescriptor, Errno> eventfd(int initval, int flags) {
        if (int fd = ::eventfd(initval, flags) < 0) {
            return std::unexpected {errno};
        } else {
            return FileDescriptor(fd);
        }
    }

    static std::expected<FileDescriptor, Errno> timerfd(clockid_t clock, int flags) {
        if (int fd = ::timerfd_create(clock, flags) < 0) {
            return std::unexpected {errno};
        } else {
            return FileDescriptor(fd);
        }
    }

    static std::expected<FileDescriptor, Errno> epollfd() {
        if (int fd = ::epoll_create1(0) < 0) {
            return std::unexpected {errno};
        } else {
            return FileDescriptor(fd);
        }
    }

    std::optional<Errno> listen(int backlog) {
        if (::listen(fd, backlog) < 0) {
            return errno;
        } else {
            return std::nullopt;
        }
    }

    std::expected<FileDescriptor, Errno> accept(sockaddr& addr, socklen_t& addrlen) {
        if (auto fd2 = ::accept(fd, &addr, &addrlen) < 0) {
            return std::unexpected {errno};
        } else {
            return FileDescriptor(fd2);
        }
    }

    std::optional<Errno> connect(const sockaddr& addr, socklen_t addrlen) {
        if (::connect(fd, &addr, addrlen) < 0) {
            return errno;
        } else {
            return std::nullopt;
        }
    }

    std::optional<Errno> bind(const sockaddr& addr, socklen_t addrlen) {
        if (::bind(fd, &addr, addrlen) < 0) {
            return errno;
        } else {
            return std::nullopt;
        }
    }

    std::expected<ssize_t, Errno> read(void* buf, size_t count) {
        ssize_t n = ::read(fd, buf, count);
        if (n < 0) {
            return std::unexpected {errno};
        } else {
            return n;
        }
    }

    std::expected<ssize_t, Errno> write(const void* buf, size_t count) {
        ssize_t n = ::write(fd, buf, count);
        if (n < 0) {
            return std::unexpected {errno};
        } else {
            return n;
        }
    }

    std::optional<Errno> eventfd_signal() {
        uint64_t u = 1;
        if (::eventfd_write(fd, u) < 0) {
            return errno;
        } else {
            return std::nullopt;
        }
    }

    std::optional<Errno> eventfd_wait() {
        uint64_t u;
        if (::eventfd_read(fd, &u) < 0) {
            return errno;
        } else {
            return std::nullopt;
        }
    }

    std::optional<Errno> timerfd_settime(const itimerspec& new_value, itimerspec* old_value = nullptr) {
        if (::timerfd_settime(fd, 0, &new_value, old_value) < 0) {
            return errno;
        } else {
            return std::nullopt;
        }
    }

    std::optional<Errno> timerfd_wait() {
        uint64_t u;
        if (::read(fd, &u, sizeof(u)) < 0) {
            return errno;
        } else {
            return std::nullopt;
        }
    }

    std::optional<Errno> epoll_ctl(int op, FileDescriptor& other, epoll_event* event) {
        if (::epoll_ctl(fd, op, other.fd, event) < 0) {
            return errno;
        } else {
            return std::nullopt;
        }
    }

    std::expected<int, Errno> epoll_wait(epoll_event* events, int maxevents, int timeout) {
        if (auto n = ::epoll_wait(fd, events, maxevents, timeout) < 0) {
            return errno;
        } else {
            return n;
        }
    }
};

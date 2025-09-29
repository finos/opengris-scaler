#pragma once

// Python
#include <stdexcept>

#include "scaler/io/ymq/pymod_ymq/python.h"

// C++
#include <memory>

// C
#include <sys/eventfd.h>
#include <sys/poll.h>

// First-party
#include "scaler/io/ymq/common.h"
#include "scaler/io/ymq/pymod_ymq/ymq.h"

// an owned handle to a PyObject with automatic reference counting via RAII
template <typename T = PyObject>
class OwnedPyObject {
public:
    OwnedPyObject(): _ptr(nullptr) {}

    // steals a reference
    OwnedPyObject(T* ptr): _ptr(ptr) {}

    OwnedPyObject(const OwnedPyObject& other) { this->_ptr = Py_XNewRef(other._ptr); }
    OwnedPyObject(OwnedPyObject&& other) noexcept: _ptr(other._ptr) { other._ptr = nullptr; }
    OwnedPyObject& operator=(const OwnedPyObject& other)
    {
        if (this == &other)
            return *this;

        this->free();
        this->_ptr = Py_XNewRef(other._ptr);
        return *this;
    }
    OwnedPyObject& operator=(OwnedPyObject&& other) noexcept
    {
        if (this == &other)
            return *this;

        this->free();
        this->_ptr = other._ptr;
        other._ptr = nullptr;
        return *this;
    }

    ~OwnedPyObject() { this->free(); }

    // creates a new OwnedPyObject from a borrowed reference
    static OwnedPyObject fromBorrowed(T* ptr) { return OwnedPyObject((T*)Py_XNewRef((PyObject*)ptr)); }

    // convenience method for creating an OwnedPyObject that holds Py_None
    static OwnedPyObject none() { return OwnedPyObject((T*)Py_NewRef(Py_None)); }

    bool is_none() const { return (PyObject*)_ptr == Py_None; }

    // takes the pointer out of the OwnedPyObject
    // without decrementing the reference count
    // use this to transfer ownership to C code
    T* take()
    {
        T* ptr     = this->_ptr;
        this->_ptr = nullptr;
        return ptr;
    }

    void forget() { this->_ptr = nullptr; }

    // operator T*() const { return _ptr; }
    explicit operator bool() const { return _ptr != nullptr; }
    bool operator!() const { return _ptr == nullptr; }

    T* operator->() const { return _ptr; }
    T* operator*() const { return _ptr; }

private:
    T* _ptr;

    void free()
    {
        if (!_ptr)
            return;

        if (!PyGILState_Check())
            return;

        Py_CLEAR(_ptr);
    }
};

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

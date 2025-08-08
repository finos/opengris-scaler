#pragma once

// Python
#include <print>
#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "scaler/io/ymq/common.h"

// an owned handle to a PyObject with automatic reference counting via RAII
template <typename T = PyObject>
class OwnedPyObject {
    T* _ptr;

public:
    OwnedPyObject(): _ptr(nullptr) {}

    // steals a reference
    OwnedPyObject(T* ptr): _ptr(ptr) {}

    OwnedPyObject(const OwnedPyObject& other) { this->_ptr = Py_XNewRef(other._ptr); }
    OwnedPyObject(OwnedPyObject&& other) noexcept: _ptr(other._ptr) { other._ptr = nullptr; }
    OwnedPyObject& operator=(const OwnedPyObject& other)
    {
        this->_ptr = Py_XNewRef(other._ptr);
        return *this;
    }
    OwnedPyObject& operator=(OwnedPyObject&& other) noexcept
    {
        this->_ptr = other._ptr;
        other._ptr = nullptr;
        return *this;
    }

    ~OwnedPyObject()
    {
        if (!_ptr)
            return;
        std::println("PYOBJ FREE: {}; refcnt: {}", (void*)_ptr, Py_REFCNT(_ptr));

        if (!PyGILState_Check()) {
            // return;
            std::println("PYOBJ FREE WITHOUT GIL");
            return;
        }
        // std::println("PYOBJ FREE WITHOUT GIL");

        Py_CLEAR(_ptr);
    }

    // creates a new OwnedPyObject from a borrowed reference
    static OwnedPyObject fromBorrowed(PyObject* ptr) { return OwnedPyObject(Py_XNewRef(ptr)); }

    // takes the pointer out of the OwnedPyObject
    // without decrementing the reference count
    // you should only use this if strictly necessary
    T* take()
    {
        T* ptr     = this->_ptr;
        this->_ptr = nullptr;
        return ptr;
    }

    void forget() { this->_ptr = nullptr; }

    operator T*() const { return _ptr; }
    explicit operator bool() const { return _ptr != nullptr; }
    bool operator!() const { return _ptr == nullptr; }

    T* operator->() const { return _ptr; }
    T* operator*() const { return _ptr; }
};

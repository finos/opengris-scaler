#pragma once

// Python
#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "scaler/io/ymq/common.h"

// an owned handle to a PyObject with automatic reference counting via RAII
template <typename T = PyObject>
class OwnedPyObject {
    T* _ptr;

    void free()
    {
        if (!_ptr)
            return;

        if (!PyGILState_Check())
            return;

        Py_CLEAR(_ptr);
    }

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
    static OwnedPyObject fromBorrowed(T* ptr) { return OwnedPyObject((T*)Py_XNewRef(ptr)); }

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
};

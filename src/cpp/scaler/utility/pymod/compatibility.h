#pragma once

#define PY_SSIZE_T_CLEAN

// if on Windows and in debug mode, undefine _DEBUG before including Python.h
// this prevents issues including the debug version of the Python library
#if defined(_WIN32) && defined(_DEBUG)
#undef _DEBUG
#include <Python.h>
#define _DEBUG
#else
#include <Python.h>
#endif

#include <structmember.h>

#include "scaler/error/error.h"
#include "scaler/utility/pymod/gil.h"

namespace scaler {
namespace utility {
namespace pymod {
// NOTE: We define this no matter what version of Python we use.
// an owned handle to a PyObject with automatic reference counting via RAII
template <typename T = PyObject>
class OwnedPyObject {
public:
    OwnedPyObject(): _ptr(nullptr)
    {
    }

    // steals a reference
    OwnedPyObject(T* ptr): _ptr(ptr)
    {
    }

    OwnedPyObject(const OwnedPyObject& other)
    {
        this->_ptr = (T*)Py_XNewRef((PyObject*)other._ptr);
    }
    OwnedPyObject(OwnedPyObject&& other) noexcept: _ptr(other._ptr)
    {
        other._ptr = nullptr;
    }
    OwnedPyObject& operator=(const OwnedPyObject& other)
    {
        if (this == &other)
            return *this;

        this->free();
        this->_ptr = (T*)Py_XNewRef((PyObject*)other._ptr);
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

    ~OwnedPyObject()
    {
        this->free();
    }

    inline friend bool operator==(const OwnedPyObject<T>& x, const OwnedPyObject<T>& y)
    {
        return PyObject_RichCompareBool(x._ptr, y._ptr, Py_EQ) == 1;
    }

    // creates a new OwnedPyObject from a borrowed reference
    static OwnedPyObject fromBorrowed(T* ptr)
    {
        return OwnedPyObject((T*)Py_XNewRef((PyObject*)ptr));
    }

    // convenience method for creating an OwnedPyObject that holds Py_None
    static OwnedPyObject none()
    {
        return OwnedPyObject((T*)Py_NewRef(Py_None));
    }

    bool is_none() const
    {
        return (PyObject*)_ptr == Py_None;
    }

    // takes the pointer out of the OwnedPyObject
    // without decrementing the reference count
    // use this to transfer ownership to C code
    T* take()
    {
        T* ptr     = this->_ptr;
        this->_ptr = nullptr;
        return ptr;
    }

    void forget()
    {
        this->_ptr = nullptr;
    }

    // operator T*() const { return _ptr; }
    explicit operator bool() const
    {
        return _ptr != nullptr;
    }
    bool operator!() const
    {
        return _ptr == nullptr;
    }

    T* operator->() const
    {
        return _ptr;
    }
    T* operator*() const = delete;
    T* get() const
    {
        return _ptr;
    }

    Py_hash_t hash() const
    {
        return PyObject_Hash(_ptr);
    }

private:
    T* _ptr;

    void free()
    {
        if (!_ptr)
            return;

        // Makes sure we hold the GIL. It might happen that this OwnedPyObject gets destructed outside of a Python
        // thread. Otherwise, if the GIL is already acquired, this is almost a NO-OP.
        AcquireGIL _;

        Py_CLEAR(_ptr);
    }
};
}  // namespace pymod
}  // namespace utility
}  // namespace scaler

template <>
struct std::hash<scaler::utility::pymod::OwnedPyObject<PyObject>> {
    std::size_t operator()(const scaler::utility::pymod::OwnedPyObject<PyObject>& obj) const noexcept
    {
        return obj.hash();
    }
};

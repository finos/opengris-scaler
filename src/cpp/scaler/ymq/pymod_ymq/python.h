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
#include "scaler/ymq/pymod_ymq/gil.h"

#if PY_MAJOR_VERSION == 3 && PY_MINOR_VERSION < 10
#define Py_TPFLAGS_IMMUTABLETYPE          (0)
#define Py_TPFLAGS_DISALLOW_INSTANTIATION (0)

static inline PyObject* Py_NewRef(PyObject* obj)
{
    Py_INCREF(obj);
    return obj;
}

static inline PyObject* Py_XNewRef(PyObject* obj)
{
    Py_XINCREF(obj);
    return obj;
}

static inline int PyModule_AddObjectRef(PyObject* mod, const char* name, PyObject* value)
{
    Py_INCREF(value);  // Since PyModule_AddObject steals a ref, we balance it
    return PyModule_AddObject(mod, name, value);
}
#endif  // <3.10

#if PY_MAJOR_VERSION == 3 && PY_MINOR_VERSION < 9
// This is a very dirty hack, we basically place the raw pointer to this dict when init,
// see scaler/ymq/pymod_ymq/ymq.h for more detail
PyObject* PyType_GetModule(PyTypeObject* type)
{
    return PyObject_GetAttrString((PyObject*)(type), "__module_object__");
}

PyObject* PyType_GetModuleState(PyTypeObject* type)
{
    PyObject* module = PyObject_GetAttrString((PyObject*)(type), "__module_object__");
    if (!module)
        return nullptr;
    void* state = PyModule_GetState(module);
    Py_DECREF(module);
    return (PyObject*)state;
}

static inline PyObject* PyObject_CallNoArgs(PyObject* callable)
{
    return PyObject_Call(callable, PyTuple_New(0), nullptr);
}

static inline PyObject* PyType_FromModuleAndSpec(PyObject* pymodule, PyType_Spec* spec, PyObject* bases)
{
    (void)pymodule;
    if (!bases) {
        bases = PyTuple_Pack(1, (PyObject*)&PyBaseObject_Type);
        if (bases) {
            PyObject* res = PyType_FromSpecWithBases(spec, bases);
            Py_DECREF(bases);  // avoid leak
            return res;
        }
        PyObject* res = PyType_FromSpecWithBases(spec, bases);
        return res;
    }
    return PyType_FromSpecWithBases(spec, bases);
}
#endif  // <3.9

// NOTE: We define this no matter what version of Python we use.
// an owned handle to a PyObject with automatic reference counting via RAII
template <typename T = PyObject>
class OwnedPyObject {
public:
    OwnedPyObject(): _ptr(nullptr) {}

    // steals a reference
    OwnedPyObject(T* ptr): _ptr(ptr) {}

    OwnedPyObject(const OwnedPyObject& other) { this->_ptr = (T*)Py_XNewRef((PyObject*)other._ptr); }
    OwnedPyObject(OwnedPyObject&& other) noexcept: _ptr(other._ptr) { other._ptr = nullptr; }
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

        // Makes sure we hold the GIL. It might happen that this OwnedPyObject gets destructed outside of a Python
        // thread. Otherwise, if the GIL is already acquired, this is almost a NO-OP.
        AcquireGIL _;

        Py_CLEAR(_ptr);
    }
};

#if PY_MAJOR_VERSION == 3 && PY_MINOR_VERSION == 8
static inline PyObject* PyObject_CallOneArg(PyObject* callable, PyObject* arg)
{
    OwnedPyObject<> args = PyTuple_Pack(1, arg);
    if (!args)
        return nullptr;
    PyObject* result = PyObject_Call(callable, *args, nullptr);
    return result;
}
#endif

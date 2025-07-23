#pragma once

// Python
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>

// C++
#include <functional>

// First-party
#include "scaler/io/ymq/pymod_ymq/ymq.h"

// wraps an async callback that accepts a Python asyncio future
static PyObject* async_wrapper(PyObject* self, const std::function<void(YMQState* state, PyObject* future)>& callback) {
    // replace with PyType_GetModuleByDef(Py_TYPE(self), &ymq_module) in a newer Python version
    // https://docs.python.org/3/c-api/type.html#c.PyType_GetModuleByDef
    PyObject* pyModule = PyType_GetModule(Py_TYPE(self));
    if (!pyModule) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to get module for Message type");
        return nullptr;
    }

    auto state = (YMQState*)PyModule_GetState(pyModule);
    if (!state) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to get module state");
        return nullptr;
    }

    PyObject* loop = PyObject_CallMethod(state->asyncioModule, "get_event_loop", nullptr);

    if (!loop) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to get event loop");
        return nullptr;
    }

    PyObject* future = PyObject_CallMethod(loop, "create_future", nullptr);

    if (!future) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create future");
        return nullptr;
    }

    // borrow the future, we'll decref this after the C++ thread is done
    Py_INCREF(future);

    // async
    callback(state, future);

    return PyObject_CallFunction(state->PyAwaitableType, "O", future);
}

struct Awaitable {
    PyObject_HEAD;
    PyObject* future;
};

extern "C" {

static int Awaitable_init(Awaitable* self, PyObject* args, PyObject* kwds) {
    if (!PyArg_ParseTuple(args, "O", &self->future)) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to parse arguments for Iterable");
        return -1;
    }

    return 0;
}

static PyObject* Awaitable_await(Awaitable* self) {
    // Easy: coroutines are just iterators and we don't need anything fancy
    // so we can just return the future's iterator!
    return PyObject_GetIter(self->future);
}

static void Awaitable_dealloc(Awaitable*self) {
    Py_DECREF(self->future);
    Py_TYPE(self)->tp_free((PyObject *)self);
}
}

static PyType_Slot Awaitable_slots[] = {
    {Py_tp_init, (void*)Awaitable_init}, {Py_tp_dealloc, (void*)Awaitable_dealloc}, {Py_am_await, (void*)Awaitable_await}, {0, nullptr}};

static PyType_Spec Awaitable_spec {
    .name      = "ymq.Awaitable",
    .basicsize = sizeof(Awaitable),
    .itemsize  = 0,
    .flags     = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_IMMUTABLETYPE,
    .slots     = Awaitable_slots,
};

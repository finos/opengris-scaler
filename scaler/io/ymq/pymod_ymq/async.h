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
static PyObject* async_wrapper(PyObject* self, const std::function<void(YMQState* state, PyObject* future)>& callback)
{
    auto state = YMQStateFromSelf(self);
    if (!state)
        return nullptr;

    PyObject* loop = PyObject_CallMethod(state->asyncioModule, "get_event_loop", nullptr);
    if (!loop) {
        Py_DECREF(loop);

        PyErr_SetString(PyExc_RuntimeError, "Failed to get event loop");
        return nullptr;
    }

    PyObject* future = PyObject_CallMethod(loop, "create_future", nullptr);

    Py_DECREF(loop);

    if (!future) {
        Py_DECREF(future);

        PyErr_SetString(PyExc_RuntimeError, "Failed to create future");
        return nullptr;
    }

    // async
    callback(state, future);

    return PyObject_CallFunction(state->PyAwaitableType, "O", future);
}

struct Awaitable {
    PyObject_HEAD;
    PyObject* future;
};

extern "C" {

static int Awaitable_init(Awaitable* self, PyObject* args, PyObject* kwds)
{
    if (!PyArg_ParseTuple(args, "O", &self->future))
        return -1;

    // we store an owned reference to the future
    Py_INCREF(self->future);

    return 0;
}

static PyObject* Awaitable_await(Awaitable* self)
{
    // Easy: coroutines are just iterators and we don't need anything fancy
    // so we can just return the future's iterator!
    return PyObject_GetIter(self->future);
}

static void Awaitable_dealloc(Awaitable* self)
{
    // destroy our owned reference
    Py_DECREF(self->future);

    auto* tp = Py_TYPE(self);
    tp->tp_free(self);
    Py_DECREF(tp);
}
}

static PyType_Slot Awaitable_slots[] = {
    {Py_tp_init, (void*)Awaitable_init},
    {Py_tp_dealloc, (void*)Awaitable_dealloc},
    {Py_am_await, (void*)Awaitable_await},
    {0, nullptr},
};

static PyType_Spec Awaitable_spec {
    .name      = "ymq.Awaitable",
    .basicsize = sizeof(Awaitable),
    .itemsize  = 0,
    .flags     = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_IMMUTABLETYPE,
    .slots     = Awaitable_slots,
};

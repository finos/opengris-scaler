#pragma once

// Python
#include "scaler/io/ymq/pymod_ymq/python.h"

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

    OwnedPyObject loop = PyObject_CallMethod(*state->asyncioModule, "get_event_loop", nullptr);
    if (!loop) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to get event loop");
        return nullptr;
    }

    OwnedPyObject future = PyObject_CallMethod(*loop, "create_future", nullptr);

    if (!future) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create future");
        return nullptr;
    }

    // create the awaitable before calling the callback
    // this ensures that we create a new strong reference to the future before the callback decrefs it
    auto awaitable = PyObject_CallFunction(*state->PyAwaitableType, "O", *future);

    // async
    // we transfer ownership of the future to the callback
    // TODO: investigate having the callback take an OwnedPyObject, and just std::move()
    callback(state, future.take());

    return awaitable;
}

struct Awaitable {
    PyObject_HEAD;
    OwnedPyObject<> future;
};

extern "C" {

static int Awaitable_init(Awaitable* self, PyObject* args, PyObject* kwds)
{
    PyObject* future = nullptr;
    if (!PyArg_ParseTuple(args, "O", &future))
        return -1;

    new (&self->future) OwnedPyObject<>();
    self->future = OwnedPyObject<>::fromBorrowed(future);

    return 0;
}

static PyObject* Awaitable_await(Awaitable* self)
{
    // Easy: coroutines are just iterators and we don't need anything fancy
    // so we can just return the future's iterator!
    return PyObject_GetIter(*self->future);
}

static void Awaitable_dealloc(Awaitable* self)
{
    try {
        self->future.~OwnedPyObject();
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to deallocate Awaitable");
        PyErr_WriteUnraisable((PyObject*)self);
    }

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

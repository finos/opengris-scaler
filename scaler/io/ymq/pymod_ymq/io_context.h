#pragma once

// Python
#include "scaler/io/ymq/pymod_ymq/python.h"

// C++
#include <functional>
#include <future>
#include <memory>

// First-party
#include "scaler/io/ymq/configuration.h"
#include "scaler/io/ymq/io_context.h"
#include "scaler/io/ymq/io_socket.h"
#include "scaler/io/ymq/pymod_ymq/io_socket.h"
#include "scaler/io/ymq/pymod_ymq/ymq.h"

// TODO: move ymq's python module into this namespace
using namespace scaler::ymq;
using Identity = Configuration::IOSocketIdentity;

struct PyIOContext {
    PyObject_HEAD;
    std::shared_ptr<IOContext> ioContext;
};

extern "C" {

static int PyIOContext_init(PyIOContext* self, PyObject* args, PyObject* kwds)
{
    // default to 1 thread if not specified
    Py_ssize_t numThreads = 1;
    const char* kwlist[]  = {"num_threads", nullptr};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "|n", (char**)kwlist, &numThreads))
        return -1;

    try {
        new (&self->ioContext) std::shared_ptr<IOContext>();
        self->ioContext = std::make_shared<IOContext>(numThreads);
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create IOContext");
        return -1;
    }

    return 0;
}

static void PyIOContext_dealloc(PyIOContext* self)
{
    try {
        self->ioContext.~shared_ptr();
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to deallocate IOContext");
        PyErr_WriteUnraisable((PyObject*)self);
    }

    auto* tp = Py_TYPE(self);
    tp->tp_free(self);
    Py_DECREF(tp);
}

static PyObject* PyIOContext_repr(PyIOContext* self)
{
    return PyUnicode_FromFormat("<IOContext at %p>", (void*)self->ioContext.get());
}

static PyObject* PyIOContext_createIOSocket_(
    PyIOContext* self,
    PyObject* args,
    PyObject* kwargs,
    std::function<PyObject*(PyIOSocket* ioSocket, Identity identity, IOSocketType socketType)> fn)
{
    const char* identity   = nullptr;
    Py_ssize_t identityLen = 0;
    PyObject* pySocketType = nullptr;
    const char* kwlist[]   = {"identity", "pySocketType", nullptr};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s#O", (char**)kwlist, &identity, &identityLen, &pySocketType))
        return nullptr;

    YMQState* state = YMQStateFromSelf((PyObject*)self);

    if (!state)
        return nullptr;

    if (!PyObject_IsInstance(pySocketType, *state->PyIOSocketEnumType)) {
        PyErr_SetString(PyExc_TypeError, "Expected socket_type to be an instance of IOSocketType");
        return nullptr;
    }

    OwnedPyObject value = PyObject_GetAttrString(pySocketType, "value");
    if (!value)
        return nullptr;

    if (!PyLong_Check(*value)) {
        PyErr_SetString(PyExc_TypeError, "Expected socket_type to be an integer");
        return nullptr;
    }

    long socketTypeValue = PyLong_AsLong(*value);

    if (socketTypeValue < 0 && PyErr_Occurred())
        return nullptr;

    IOSocketType socketType = static_cast<IOSocketType>(socketTypeValue);

    OwnedPyObject<PyIOSocket> ioSocket = PyObject_New(PyIOSocket, (PyTypeObject*)*state->PyIOSocketType);
    if (!ioSocket)
        return nullptr;

    try {
        // ensure the fields are init
        new (&ioSocket->socket) std::shared_ptr<IOSocket>();
        new (&ioSocket->ioContext) std::shared_ptr<IOContext>();
        ioSocket->ioContext = self->ioContext;
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create IOSocket");
        return nullptr;
    }

    // move ownership of the ioSocket to the callback
    return fn(ioSocket.take(), identity, socketType);
}

static PyObject* PyIOContext_createIOSocket(PyIOContext* self, PyObject* args, PyObject* kwargs)
{
    return PyIOContext_createIOSocket_(
        self, args, kwargs, [self](auto ioSocket, Identity identity, IOSocketType socketType) {
            return async_wrapper((PyObject*)self, [=](YMQState* state, auto future) {
                self->ioContext->createIOSocket(identity, socketType, [=](std::shared_ptr<IOSocket> socket) {
                    future_set_result(future, [=] {
                        ioSocket->socket = std::move(socket);
                        return (PyObject*)ioSocket;
                    });
                });
            });
        });
}

static PyObject* PyIOContext_createIOSocket_sync(PyIOContext* self, PyObject* args, PyObject* kwargs)
{
    auto state = YMQStateFromSelf((PyObject*)self);
    if (!state)
        return nullptr;

    return PyIOContext_createIOSocket_(
        self, args, kwargs, [self, state](auto ioSocket, Identity identity, IOSocketType socketType) {
            PyThreadState* _save = PyEval_SaveThread();

            std::shared_ptr<IOSocket> socket {};
            try {
                Waiter waiter(state->wakeupfd_rd);

                self->ioContext->createIOSocket(
                    identity, socketType, [waiter, &socket](std::shared_ptr<IOSocket> s) mutable {
                        socket = std::move(s);
                        waiter.signal();
                    });

                WAIT(waiter);
            } catch (...) {
                PyEval_RestoreThread(_save);
                PyErr_SetString(PyExc_RuntimeError, "Failed to create io socket synchronously");
                return (PyObject*)nullptr;
            }

            PyEval_RestoreThread(_save);

            ioSocket->socket = socket;
            return (PyObject*)ioSocket;
        });
}

static PyObject* PyIOContext_numThreads_getter(PyIOContext* self, void* Py_UNUSED(closure))
{
    return PyLong_FromSize_t(self->ioContext->numThreads());
}

}  // extern "C"

static PyMethodDef PyIOContext_methods[] = {
    {"createIOSocket",
     (PyCFunction)PyIOContext_createIOSocket,
     METH_VARARGS | METH_KEYWORDS,
     PyDoc_STR("Create a new IOSocket")},
    {"createIOSocket_sync",
     (PyCFunction)PyIOContext_createIOSocket_sync,
     METH_VARARGS | METH_KEYWORDS,
     PyDoc_STR("Create a new IOSocket")},
    {nullptr, nullptr, 0, nullptr},
};

static PyGetSetDef PyIOContext_properties[] = {
    {"num_threads",
     (getter)PyIOContext_numThreads_getter,
     nullptr,
     PyDoc_STR("Get the number of threads in the IOContext"),
     nullptr},
    {nullptr, nullptr, nullptr, nullptr, nullptr},
};

static PyType_Slot PyIOContext_slots[] = {
    {Py_tp_init, (void*)PyIOContext_init},
    {Py_tp_dealloc, (void*)PyIOContext_dealloc},
    {Py_tp_repr, (void*)PyIOContext_repr},
    {Py_tp_methods, (void*)PyIOContext_methods},
    {Py_tp_getset, (void*)PyIOContext_properties},
    {0, nullptr},
};

static PyType_Spec PyIOContext_spec = {
    .name      = "ymq.IOContext",
    .basicsize = sizeof(PyIOContext),
    .itemsize  = 0,
    .flags     = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_IMMUTABLETYPE,
    .slots     = PyIOContext_slots,
};

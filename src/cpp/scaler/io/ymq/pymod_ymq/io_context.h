#pragma once

// Python
#include "scaler/io/ymq/pymod_ymq/python.h"

// C++
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

static PyObject* PyIOContext_numThreads_getter(PyIOContext* self, void* Py_UNUSED(closure))
{
    return PyLong_FromSize_t(self->ioContext->numThreads());
}

static PyObject* PyIOContext_createIOSocket(PyIOContext* self, PyObject* args, PyObject* kwargs)
{
    YMQState* state = YMQStateFromSelf((PyObject*)self);
    if (!state)
        return nullptr;

    PyObject* callback     = nullptr;
    const char* identity   = nullptr;
    Py_ssize_t identityLen = 0;
    PyObject* pySocketType = nullptr;
    const char* kwlist[]   = {"", "identity", "socket_type", nullptr};
    if (!PyArg_ParseTupleAndKeywords(
            args, kwargs, "Os#O", (char**)kwlist, &callback, &identity, &identityLen, &pySocketType))
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

    IOSocketType socketType            = static_cast<IOSocketType>(socketTypeValue);
    OwnedPyObject<PyIOSocket> ioSocket = PyObject_New(PyIOSocket, (PyTypeObject*)*state->PyIOSocketType);
    if (!ioSocket)
        return nullptr;

    try {
        // ensure the fields are init
        new (&ioSocket->socket) std::shared_ptr<IOSocket>();
        new (&ioSocket->ioContext) std::shared_ptr<IOContext>();
        ioSocket->ioContext = self->ioContext;

        Py_INCREF(callback);

        self->ioContext->createIOSocket(
            std::string(identity, identityLen), socketType, [callback, ioSocket](auto socket) {
                AcquireGIL _;

                ioSocket->socket      = socket;
                OwnedPyObject _result = PyObject_CallFunctionObjArgs(callback, *ioSocket, nullptr);

                Py_DECREF(callback);
            });
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create IOSocket");
        return nullptr;
    }

    Py_RETURN_NONE;
}

}  // extern "C"

static PyMethodDef PyIOContext_methods[] = {
    {"createIOSocket",
     (PyCFunction)PyIOContext_createIOSocket,
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
    .name      = "_ymq.BaseIOContext",
    .basicsize = sizeof(PyIOContext),
    .itemsize  = 0,
    .flags     = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_IMMUTABLETYPE | Py_TPFLAGS_BASETYPE,
    .slots     = PyIOContext_slots,
};

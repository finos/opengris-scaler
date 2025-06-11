#pragma once

// Python
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>

// C++
#include <memory>

// First-party
#include "scaler/io/ymq/io_socket.h"
#include "scaler/io/ymq/pymod_ymq/ymq.h"

struct PyIOSocket {
    PyObject_HEAD;
    std::shared_ptr<IOSocket> socket;
};

extern "C" {

static int PyIOSocket_init(PyIOSocket* self, PyObject* args, PyObject* kwds) {
    return 0;
}

static void PyIOSocket_dealloc(PyIOSocket* self) {
    self->socket.~shared_ptr();               // Call the destructor of shared_ptr
    Py_TYPE(self)->tp_free((PyObject*)self);  // Free the PyObject
}

static PyObject* PyIOSocket_send(PyIOSocket* self, PyObject* args, PyObject* kwargs) {
    PyErr_SetString(PyExc_NotImplementedError, "send() is not implemented yet");
    return nullptr;

    // in this function we need to:
    // 1. create an asyncio future (easy)
    //    - should be pretty easy, just call standard methods
    // 2. create a handle to that future that can be completed in a C++ callback (harder)
    //    - how do we call Python from non-Python threads? where is the handle stored?
    // 3. await the future (very hard)
    //    - how do you await in a C extension module? is it even possible?
    //    - might need to call into Python code to make this work
}

static PyObject* PyIOSocket_repr(PyIOSocket* self) {
    return PyUnicode_FromFormat("<IOSocket at %p>", (void*)self->socket.get());
}

static PyObject* PyIOSocket_identity_getter(PyIOSocket* self, void* closure) {
    return PyUnicode_FromStringAndSize(self->socket->identity().data(), self->socket->identity().size());
}

static PyObject* PyIOSocket_socket_type_getter(PyIOSocket* self, void* closure) {
    // replace with PyType_GetModuleByDef(Py_TYPE(self), &ymq_module) in a newer Python version
    // https://docs.python.org/3/c-api/type.html#c.PyType_GetModuleByDef
    PyObject* module = PyType_GetModule(Py_TYPE(self));
    if (!module) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to get module for Message type");
        return nullptr;
    }

    auto state = (YmqState*)PyModule_GetState(module);
    if (!state) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to get module state");
        return nullptr;
    }

    IOSocketType socketType = self->socket->socketType();
    PyObject* socketTypeObj = PyLong_FromLong((long)socketType);

    if (!socketTypeObj) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to convert socket type to a Python integer");
        return nullptr;
    }

    return socketTypeObj;
}
}

static PyGetSetDef PyIOSocket_properties[] = {
    {"identity", (getter)PyIOSocket_identity_getter, nullptr, PyDoc_STR("Get the identity of the IOSocket"), nullptr},
    {"socket_type", (getter)PyIOSocket_socket_type_getter, nullptr, PyDoc_STR("Get the type of the IOSocket"), nullptr},
    {nullptr, nullptr, nullptr, nullptr, nullptr}};

static PyMethodDef PyIOSocket_methods[] = {
    {"send", (PyCFunction)PyIOSocket_send, METH_VARARGS | METH_KEYWORDS, PyDoc_STR("Send data through the IOSocket")},
    {nullptr, nullptr, 0, nullptr}};

static PyType_Slot PyIOSocket_slots[] = {
    {Py_tp_init, (void*)PyIOSocket_init},
    {Py_tp_dealloc, (void*)PyIOSocket_dealloc},
    {Py_tp_repr, (void*)PyIOSocket_repr},
    {Py_tp_getset, (void*)PyIOSocket_properties},
    {Py_tp_methods, (void*)PyIOSocket_methods},
    {0, nullptr},
};

static PyType_Spec PyIOSocket_spec = {
    .name      = "ymq.IOSocket",
    .basicsize = sizeof(PyIOSocket),
    .itemsize  = 0,
    .flags     = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_IMMUTABLETYPE,
    .slots     = PyIOSocket_slots,
};

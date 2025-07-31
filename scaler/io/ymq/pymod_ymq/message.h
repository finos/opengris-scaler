#pragma once

// Python
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>

// First-party
#include "scaler/io/ymq/pymod_ymq/bytes.h"
#include "scaler/io/ymq/pymod_ymq/ymq.h"

struct PyMessage {
    PyObject_HEAD;
    PyBytesYMQ* address;  // Address of the message
    PyBytesYMQ* payload;  // Payload of the message
};

extern "C" {

static int PyMessage_init(PyMessage* self, PyObject* args, PyObject* kwds) {
    // replace with PyType_GetModuleByDef(Py_TYPE(self), &ymq_module) in a newer Python version
    // https://docs.python.org/3/c-api/type.html#c.PyType_GetModuleByDef
    PyObject* pyModule = PyType_GetModule(Py_TYPE(self));
    if (!pyModule) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to get module for Message type");
        return -1;
    }

    auto state = (YMQState*)PyModule_GetState(pyModule);
    if (!state) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to get module state");
        return -1;
    }

    PyObject *address = nullptr, *payload = nullptr;
    const char* keywords[] = {"address", "payload", nullptr};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "OO", (char**)keywords, &address, &payload)) {
        PyErr_SetString(PyExc_TypeError, "Expected two Bytes objects: address and payload");
        return -1;
    }

    // check if the address and payload are of type PyBytesYMQ
    if (!PyObject_IsInstance(address, state->PyBytesYMQType)) {
        PyObject* args = PyTuple_Pack(1, address);
        address        = PyObject_CallObject(state->PyBytesYMQType, args);
        Py_DECREF(args);

        if (!address) {
            return -1;
        }
    }

    if (!PyObject_IsInstance(payload, state->PyBytesYMQType)) {
        PyObject* args = PyTuple_Pack(1, payload);
        payload        = PyObject_CallObject(state->PyBytesYMQType, args);
        Py_DECREF(args);

        if (!payload) {
            return -1;
        }
    }

    self->address = (PyBytesYMQ*)address;
    self->payload = (PyBytesYMQ*)payload;

    return 0;
}

static void PyMessage_dealloc(PyMessage* self) {
    Py_XDECREF(self->address);
    Py_XDECREF(self->payload);
    Py_TYPE(self)->tp_free(self);
}

static PyObject* PyMessage_repr(PyMessage* self) {
    return PyUnicode_FromFormat("<Message address=%R payload=%R>", self->address, self->payload);
}
}

static PyMemberDef PyMessage_members[] = {
    {"address", T_OBJECT, offsetof(PyMessage, address), 0, PyDoc_STR("the address of the message")},
    {"payload", T_OBJECT, offsetof(PyMessage, payload), 0, PyDoc_STR("the payload of the message")},
    {nullptr},
};

static PyType_Slot PyMessage_slots[] = {
    {Py_tp_init, (void*)PyMessage_init},
    {Py_tp_dealloc, (void*)PyMessage_dealloc},
    {Py_tp_repr, (void*)PyMessage_repr},
    {Py_tp_members, (void*)PyMessage_members},
    {0, nullptr},
};

static PyType_Spec PyMessage_spec = {
    .name      = "ymq.Message",
    .basicsize = sizeof(PyMessage),
    .itemsize  = 0,
    .flags     = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_IMMUTABLETYPE,
    .slots     = PyMessage_slots,
};

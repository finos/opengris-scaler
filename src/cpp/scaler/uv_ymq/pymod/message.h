#pragma once

// Python
#include "scaler/utility/pymod/compatibility.h"

// First-party
#include "scaler/uv_ymq/pymod/bytes.h"
#include "scaler/uv_ymq/pymod/uv_ymq.h"

namespace scaler {
namespace uv_ymq {
namespace pymod {

using scaler::utility::pymod::OwnedPyObject;

struct PyMessage {
    PyObject_HEAD;
    OwnedPyObject<PyBytes> address;  // Address of the message; can be None
    OwnedPyObject<PyBytes> payload;  // Payload of the message
};

static int PyMessage_init(PyMessage* self, PyObject* args, PyObject* kwds)
{
    auto state = UVYMQStateFromSelf((PyObject*)self);
    if (!state)
        return -1;

    PyObject* address      = nullptr;
    PyObject* payload      = nullptr;
    const char* keywords[] = {"address", "payload", nullptr};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "OO", (char**)keywords, &address, &payload))
        return -1;

    // address can be None, which means the message has no address
    // check if the address and payload are of type PyBytes
    if (PyObject_IsInstance(address, *state->PyBytesType)) {
        self->address = OwnedPyObject<PyBytes>::fromBorrowed((PyBytes*)address);
    } else if (address == Py_None) {
        self->address = OwnedPyObject<PyBytes>::none();
    } else {
        OwnedPyObject args = PyTuple_Pack(1, address);
        self->address      = (PyBytes*)PyObject_CallObject(*state->PyBytesType, *args);

        if (!self->address)
            return -1;
    }

    if (PyObject_IsInstance(payload, *state->PyBytesType)) {
        self->payload = OwnedPyObject<PyBytes>::fromBorrowed((PyBytes*)payload);
    } else {
        OwnedPyObject args = PyTuple_Pack(1, payload);
        self->payload      = (PyBytes*)PyObject_CallObject(*state->PyBytesType, *args);

        if (!self->payload) {
            return -1;
        }
    }

    return 0;
}

static void PyMessage_dealloc(PyMessage* self)
{
    try {
        self->address.~OwnedPyObject();
        self->payload.~OwnedPyObject();
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to deallocate Message");
        PyErr_WriteUnraisable((PyObject*)self);
    }

    auto* tp = Py_TYPE(self);
    tp->tp_free(self);
    Py_DECREF(tp);
}

static PyObject* PyMessage_repr(PyMessage* self)
{
    return PyUnicode_FromFormat("<Message address=%R payload=%R>", *self->address, *self->payload);
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
    .name      = "_uv_ymq.Message",
    .basicsize = sizeof(PyMessage),
    .itemsize  = 0,
    .flags     = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_IMMUTABLETYPE,
    .slots     = PyMessage_slots,
};

}  // namespace pymod
}  // namespace uv_ymq
}  // namespace scaler

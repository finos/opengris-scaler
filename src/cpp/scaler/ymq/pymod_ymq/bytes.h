#pragma once

// Python
#include "scaler/ymq/pymod_ymq/compatibility.h"

// First-party
#include "scaler/ymq/bytes.h"

namespace scaler {
namespace ymq {

struct PyBytesYMQ {
    PyObject_HEAD;
    scaler::ymq::Bytes bytes;
};

extern "C" {

static int PyBytesYMQ_init(PyBytesYMQ* self, PyObject* args, PyObject* kwds)
{
    Py_buffer view {.buf = nullptr};
    const char* keywords[] = {"bytes", nullptr};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "|y*", (char**)keywords, &view))
        return -1;  // Error parsing arguments

    if (!view.buf) {
        // If no bytes were provided, initialize with an empty Bytes object
        self->bytes = scaler::ymq::Bytes();
        return 0;
    }

    // copy the data into the Bytes object
    // it might be possible to make this zero-copy in the future
    self->bytes = scaler::ymq::Bytes((char*)view.buf, view.len);

    PyBuffer_Release(&view);
    return 0;
}

static void PyBytesYMQ_dealloc(PyBytesYMQ* self)
{
    try {
        self->bytes.~Bytes();  // Call the destructor to free the Bytes object
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to deallocate Bytes");
        PyErr_WriteUnraisable((PyObject*)self);
    }

    auto* tp = Py_TYPE(self);
    tp->tp_free(self);
    Py_DECREF(tp);
}

static PyObject* PyBytesYMQ_repr(PyBytesYMQ* self)
{
    if (self->bytes.is_null()) {
        return PyUnicode_FromString("<Bytes: empty>");
    } else {
        return PyUnicode_FromFormat("<Bytes: %db>", self->bytes.len());
    }
}

static PyObject* PyBytesYMQ_data_getter(PyBytesYMQ* self)
{
    if (self->bytes.is_null())
        Py_RETURN_NONE;

    return PyBytes_FromStringAndSize((const char*)self->bytes.data(), self->bytes.len());
}

static Py_ssize_t PyBytesYMQ_len(PyBytesYMQ* self)
{
    return self->bytes.len();
}

static PyObject* PyBytesYMQ_len_getter(PyBytesYMQ* self)
{
    return PyLong_FromSize_t(self->bytes.len());
}

static int PyBytesYMQ_getbuffer(PyBytesYMQ* self, Py_buffer* view, int flags)
{
    return PyBuffer_FillInfo(view, (PyObject*)self, (void*)self->bytes.data(), self->bytes.len(), true, flags);
}

static void PyBytesYMQ_releasebuffer(PyBytesYMQ* self, Py_buffer* view)
{
}
}

static PyGetSetDef PyBytesYMQ_properties[] = {
    {"data", (getter)PyBytesYMQ_data_getter, nullptr, PyDoc_STR("Data of the Bytes object"), nullptr},
    {"len", (getter)PyBytesYMQ_len_getter, nullptr, PyDoc_STR("Length of the Bytes object"), nullptr},
    {nullptr, nullptr, nullptr, nullptr, nullptr},  // Sentinel
};

static PyType_Slot PyBytesYMQ_slots[] = {
    {Py_tp_init, (void*)PyBytesYMQ_init},
    {Py_tp_dealloc, (void*)PyBytesYMQ_dealloc},
    {Py_tp_repr, (void*)PyBytesYMQ_repr},
    {Py_mp_length, (void*)PyBytesYMQ_len},
    {Py_tp_getset, (void*)PyBytesYMQ_properties},
#if PY_MAJOR_VERSION == 3 && PY_MINOR_VERSION > 8
    {Py_bf_getbuffer, (void*)PyBytesYMQ_getbuffer},
    {Py_bf_releasebuffer, (void*)PyBytesYMQ_releasebuffer},
#endif
    {0, nullptr},
};

static PyType_Spec PyBytesYMQ_spec = {
    .name      = "_ymq.Bytes",
    .basicsize = sizeof(PyBytesYMQ),
    .itemsize  = 0,
    .flags     = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_IMMUTABLETYPE,
    .slots     = PyBytesYMQ_slots,
};

}  // namespace ymq
}  // namespace scaler

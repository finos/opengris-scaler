#pragma once

// Python
#include "scaler/utility/pymod/compatibility.h"

// First-party
#include "scaler/ymq/bytes.h"

namespace scaler {
namespace uv_ymq {
namespace pymod {

struct PyBytes {
    PyObject_HEAD;
    scaler::ymq::Bytes bytes;
};

static int PyBytes_init(PyBytes* self, PyObject* args, PyObject* kwds)
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

static void PyBytes_dealloc(PyBytes* self)
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

static PyObject* PyBytes_repr(PyBytes* self)
{
    if (self->bytes.is_null()) {
        return PyUnicode_FromString("<Bytes: empty>");
    } else {
        return PyUnicode_FromFormat("<Bytes: %db>", self->bytes.len());
    }
}

static PyObject* PyBytes_data_getter(PyBytes* self)
{
    if (self->bytes.is_null())
        Py_RETURN_NONE;

    return PyBytes_FromStringAndSize((const char*)self->bytes.data(), self->bytes.len());
}

static Py_ssize_t PyBytes_len(PyBytes* self)
{
    return self->bytes.len();
}

static PyObject* PyBytes_len_getter(PyBytes* self)
{
    return PyLong_FromSize_t(self->bytes.len());
}

static int PyBytes_getbuffer(PyBytes* self, Py_buffer* view, int flags)
{
    return PyBuffer_FillInfo(view, (PyObject*)self, (void*)self->bytes.data(), self->bytes.len(), true, flags);
}

static void PyBytes_releasebuffer(PyBytes* self, Py_buffer* view)
{
}

static PyGetSetDef PyBytes_properties[] = {
    {"data", (getter)PyBytes_data_getter, nullptr, nullptr, nullptr},
    {"len", (getter)PyBytes_len_getter, nullptr, nullptr, nullptr},
    {nullptr, nullptr, nullptr, nullptr, nullptr},  // Sentinel
};

static PyType_Slot PyBytes_slots[] = {
    {Py_tp_init, (void*)PyBytes_init},
    {Py_tp_dealloc, (void*)PyBytes_dealloc},
    {Py_tp_repr, (void*)PyBytes_repr},
    {Py_mp_length, (void*)PyBytes_len},
    {Py_tp_getset, (void*)PyBytes_properties},
#if PY_MAJOR_VERSION == 3 && PY_MINOR_VERSION > 8
    {Py_bf_getbuffer, (void*)PyBytes_getbuffer},
    {Py_bf_releasebuffer, (void*)PyBytes_releasebuffer},
#endif
    {0, nullptr},
};

static PyType_Spec PyBytes_spec = {
    .name      = "_uv_ymq.Bytes",
    .basicsize = sizeof(PyBytes),
    .itemsize  = 0,
    .flags     = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_IMMUTABLETYPE,
    .slots     = PyBytes_slots,
};

}  // namespace pymod
}  // namespace uv_ymq
}  // namespace scaler

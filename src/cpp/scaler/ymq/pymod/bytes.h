#pragma once

// Python
#include "scaler/utility/pymod/compatibility.h"

// C++
#include <memory>

// First-party
#include "scaler/ymq/bytes.h"
#include "scaler/ymq/pymod/py_buffer_bytes.h"

namespace scaler {
namespace ymq {
namespace pymod {

struct PyBytes {
    PyObject_HEAD;
    std::unique_ptr<scaler::ymq::Bytes> bytes;
};

static int PyBytes_init(PyBytes* self, PyObject* args, PyObject* kwds)
{
    Py_buffer view {};
    view.buf               = nullptr;
    const char* keywords[] = {"bytes", nullptr};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "|y*", (char**)keywords, &view))
        return -1;

    if (!view.buf) {
        return 0;
    }

    // Zero-copy: transfer Py_buffer ownership to PyBufferBytes.
    // Do NOT call PyBuffer_Release here — PyBufferBytes owns it now.
    self->bytes = std::make_unique<pymod::PyBufferBytes>(view);
    return 0;
}

static void PyBytes_dealloc(PyBytes* self)
{
    self->bytes.reset();

    auto* tp = Py_TYPE(self);
    tp->tp_free(self);
    Py_DECREF(tp);
}

static PyObject* PyBytes_repr(PyBytes* self)
{
    if (!self->bytes || !self->bytes->data()) {
        return PyUnicode_FromString("<Bytes: empty>");
    } else {
        return PyUnicode_FromFormat("<Bytes: %zdb>", self->bytes->size());
    }
}

static PyObject* PyBytes_data_getter(PyBytes* self, [[maybe_unused]] void* closure)
{
    if (!self->bytes || !self->bytes->data())
        Py_RETURN_NONE;

    return PyBytes_FromStringAndSize((const char*)self->bytes->data(), self->bytes->size());
}

static Py_ssize_t PyBytes_len(PyBytes* self)
{
    if (!self->bytes)
        return 0;
    return static_cast<Py_ssize_t>(self->bytes->size());
}

static PyObject* PyBytes_len_getter(PyBytes* self, [[maybe_unused]] void* closure)
{
    if (!self->bytes)
        return PyLong_FromSize_t(0);
    return PyLong_FromSize_t(self->bytes->size());
}

static int PyBytes_getbuffer(PyBytes* self, Py_buffer* view, int flags)
{
    void* data     = self->bytes ? self->bytes->data() : nullptr;
    Py_ssize_t len = self->bytes ? static_cast<Py_ssize_t>(self->bytes->size()) : 0;
    return PyBuffer_FillInfo(view, (PyObject*)self, data, len, true, flags);
}

static void PyBytes_releasebuffer([[maybe_unused]] PyBytes* self, [[maybe_unused]] Py_buffer* view)
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
    {Py_bf_getbuffer, (void*)PyBytes_getbuffer},
    {Py_bf_releasebuffer, (void*)PyBytes_releasebuffer},
    {0, nullptr},
};

static PyType_Spec PyBytes_spec = {
    .name      = "_ymq.Bytes",
    .basicsize = sizeof(PyBytes),
    .itemsize  = 0,
    .flags     = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_IMMUTABLETYPE,
    .slots     = PyBytes_slots,
};

}  // namespace pymod
}  // namespace ymq
}  // namespace scaler

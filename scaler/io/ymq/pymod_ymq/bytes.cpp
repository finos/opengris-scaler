// allows us to define PyTypeObjects in the canonical way without warnings
#pragma clang diagnostic ignored "-Wreorder-init-list"
#pragma clang diagnostic ignored "-Wc99-designator"

// Python
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>

struct PyBytesYmq {
    PyObject_HEAD;
};

static int PyBytesYmq_init(PyBytesYmq* self, PyObject* args, PyObject* kwds) {
    return 0;  // todo
}

static void PyBytesYmq_dealloc(PyBytesYmq* self) {
    // todo
}

static PyObject* PyBytesYmq_repr(PyBytesYmq* self) {
    Py_RETURN_NONE;  // todo
}

static PyObject* PyBytesYmq_data_getter(PyBytesYmq* self) {
    Py_RETURN_NONE;  // todo
}

static PyObject* PyBytesYmq_len_getter(PyBytesYmq* self) {
    Py_RETURN_NONE;  // todo
}

static int PyBytesYmq_getbuffer(PyBytesYmq* self, Py_buffer* view, int flags) {
    return 0;  // todo
}

static void PyBytesYmq_releasebuffer(PyBytesYmq* self, Py_buffer* view) {
    // todo
}

static PyGetSetDef PyBytesYmq_properties[] = {{nullptr, nullptr, nullptr, nullptr, nullptr}};

static PyBufferProcs PyBytesYmqBufferProcs = {
    .bf_getbuffer     = (getbufferproc)PyBytesYmq_getbuffer,
    .bf_releasebuffer = (releasebufferproc)PyBytesYmq_releasebuffer,
};

// clang-format off
static PyTypeObject PyBytesYmqType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name      = "ymq.Bytes",
    .tp_doc       = PyDoc_STR("Bytes"),
    .tp_basicsize = sizeof(PyBytesYmq),
    .tp_itemsize  = 0,
    .tp_flags     = Py_TPFLAGS_DEFAULT,
    .tp_new       = PyType_GenericNew,
    .tp_init      = (initproc)PyBytesYmq_init,
    .tp_repr      = (reprfunc)PyBytesYmq_repr,
    .tp_dealloc   = (destructor)PyBytesYmq_dealloc,
    .tp_getset    = PyBytesYmq_properties,
    .tp_as_buffer = &PyBytesYmqBufferProcs,
};
// clang-format on

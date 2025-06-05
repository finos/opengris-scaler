// allows us to define PyTypeObjects in the canonical way without warnings
#pragma clang diagnostic ignored "-Wreorder-init-list"
#pragma clang diagnostic ignored "-Wc99-designator"

// Python
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>

struct PyIOContext {
    PyObject_HEAD;
};

static int PyIOContext_init(PyIOContext* self, PyObject* args, PyObject* kwds) {
    return 0;  // todo
}

static void PyIOContext_dealloc(PyIOContext* self) {
    // todo
}

static PyObject* PyIOContext_repr(PyIOContext* self) {
    Py_RETURN_NONE;  // todo
}

static PyMethodDef PyIOContext_methods[] = {{nullptr, nullptr, 0, nullptr}};

// clang-format off
static PyTypeObject PyIOContextType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name      = "ymq.IOContext",
    .tp_doc       = PyDoc_STR("IOContext"),
    .tp_basicsize = sizeof(PyIOContext),
    .tp_itemsize  = 0,
    .tp_flags     = Py_TPFLAGS_DEFAULT,
    .tp_new       = PyType_GenericNew,
    .tp_init      = (initproc)PyIOContext_init,
    .tp_repr      = (reprfunc)PyIOContext_repr,
    .tp_dealloc   = (destructor)PyIOContext_dealloc,
    .tp_methods   = PyIOContext_methods,
};
// clang-format on

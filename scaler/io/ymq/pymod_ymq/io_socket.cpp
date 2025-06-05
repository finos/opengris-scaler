// allows us to define PyTypeObjects in the canonical way without warnings
#pragma clang diagnostic ignored "-Wreorder-init-list"
#pragma clang diagnostic ignored "-Wc99-designator"

// Python
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>

struct PyIOSocket {
    PyObject_HEAD;
};

static int PyIOSocket_init(PyIOSocket* self, PyObject* args, PyObject* kwds) {
    return 0;  // todo
}

static void PyIOSocket_dealloc(PyIOSocket* self) {
    // todo
}

static PyObject* PyIOSocket_repr(PyIOSocket* self) {
    Py_RETURN_NONE;  // todo
}

static PyObject* PyIOSocket_identity_getter(PyIOSocket* self, void* closure) {
    Py_RETURN_NONE;  // todo
}

static PyGetSetDef PyIOSocket_properties[] = {{nullptr, nullptr, nullptr, nullptr, nullptr}};

static PyMethodDef PyIOSocket_methods[] = {{nullptr, nullptr, 0, nullptr}};

// clang-format off
static PyTypeObject PyIOSocketType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name      = "ymq.IOSocket",
    .tp_doc       = PyDoc_STR("IOSocket"),
    .tp_basicsize = sizeof(PyIOSocket),
    .tp_itemsize  = 0,
    .tp_flags     = Py_TPFLAGS_DEFAULT,
    .tp_new       = PyType_GenericNew,
    .tp_init      = (initproc)PyIOSocket_init,
    .tp_repr      = (reprfunc)PyIOSocket_repr,
    .tp_dealloc   = (destructor)PyIOSocket_dealloc,
    .tp_getset    = PyIOSocket_properties,
    .tp_methods   = PyIOSocket_methods,
};
// clang-format on

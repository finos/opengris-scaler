// allows us to define PyTypeObjects in the canonical way without warnings
#pragma clang diagnostic ignored "-Wreorder-init-list"
#pragma clang diagnostic ignored "-Wc99-designator"

// Python
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>

struct PyMessage {
    PyObject_HEAD;
};

static int PyMessage_init(PyMessage* self, PyObject* args, PyObject* kwds) {
    return 0;  // todo
}

static void PyMessage_dealloc(PyMessage* self) {
    // todo
}

static PyObject* PyMessage_repr(PyMessage* self) {
    Py_RETURN_NONE;  // todo
}

static PyMemberDef PyMessage_members[] = {{nullptr}};

// clang-format off
static PyTypeObject PyMessageType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name      = "ymq.Message",
    .tp_doc       = PyDoc_STR("Message"),
    .tp_basicsize = sizeof(PyMessage),
    .tp_itemsize  = 0,
    .tp_flags     = Py_TPFLAGS_DEFAULT,
    .tp_new       = PyType_GenericNew,
    .tp_init      = (initproc)PyMessage_init,
    .tp_dealloc   = (destructor)PyMessage_dealloc,
    .tp_repr      = (reprfunc)PyMessage_repr,
    .tp_members   = PyMessage_members,
};
// clang-format on

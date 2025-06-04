// allows us to define PyTypeObjects in the canonical way without warnings
#pragma clang diagnostic ignored "-Wreorder-init-list"
#pragma clang diagnostic ignored "-Wc99-designator"

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>

// --- Module State ---

struct YmqState {};

// --- Bytes ---

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

// --- Message ---

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

// --- IOSocket ---

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
    .tp_methods   = PyIOSocket_methods
};
// clang-format on

// --- IOContext ---

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
    .tp_methods   = PyIOContext_methods
};
// clang-format on

// --- Module ---

static int ymq_exec(PyObject* module) {
    if (PyType_Ready(&PyBytesYmqType) < 0)
        return -1;

    if (PyModule_AddObjectRef(module, "Bytes", (PyObject*)&PyBytesYmqType) < 0)
        return -1;

    if (PyType_Ready(&PyMessageType) < 0)
        return -1;

    if (PyModule_AddObjectRef(module, "Message", (PyObject*)&PyMessageType) < 0)
        return -1;

    if (PyType_Ready(&PyIOSocketType) < 0)
        return -1;

    if (PyModule_AddObjectRef(module, "IoSocket", (PyObject*)&PyIOSocketType) < 0)
        return -1;

    if (PyType_Ready(&PyIOContextType) < 0)
        return -1;

    if (PyModule_AddObjectRef(module, "IoContext", (PyObject*)&PyIOContextType) < 0)
        return -1;

    auto state = (YmqState*)PyModule_GetState(module);

    // todo!

    return 0;
}

static PyMethodDef ymq_methods[] = {{NULL, NULL, 0, NULL}};

static PyModuleDef_Slot ymq_slots[] = {
    {Py_mod_exec, (void*)ymq_exec},
    // only supported in Python 3.12+
    // {Py_mod_multiple_interpreters, Py_MOD_MULTIPLE_INTERPRETERS_NOT_SUPPORTED},
    {0, NULL}};

void ymq_free(YmqState* state) {
    // todo
}

static PyModuleDef ymq_module = {
    .m_base    = PyModuleDef_HEAD_INIT,
    .m_name    = "ymq",
    .m_doc     = PyDoc_STR("YMQ Python bindings"),
    .m_size    = sizeof(YmqState),
    .m_methods = ymq_methods,
    .m_slots   = ymq_slots,
    .m_free    = (freefunc)ymq_free,
};

PyMODINIT_FUNC PyInit_ymq(void) {
    return PyModuleDef_Init(&ymq_module);
}

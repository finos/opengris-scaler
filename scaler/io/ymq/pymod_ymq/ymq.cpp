// Python
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>

// First-Party
#include "scaler/io/ymq/pymod_ymq/bytes.cpp"
#include "scaler/io/ymq/pymod_ymq/io_context.cpp"
#include "scaler/io/ymq/pymod_ymq/io_socket.cpp"
#include "scaler/io/ymq/pymod_ymq/message.cpp"

struct YmqState {};

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

static PyModuleDef_Slot ymq_slots[] = {{Py_mod_exec, (void*)ymq_exec}, {0, NULL}};

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

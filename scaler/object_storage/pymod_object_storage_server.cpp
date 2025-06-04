#include "scaler/object_storage/object_storage_server.h"
#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <string>

extern "C" {
void run_object_storage_server(const char* name, const char* port) {
    scaler::object_storage::ObjectStorageServer server;
    server.run(name, port);
}

static PyObject* object_storage_server_run_object_storage_server(PyObject* self, PyObject* args) {
    const char* addr;
    const char* port;

    if (!PyArg_ParseTuple(args, "ss", &addr, &port))
        return NULL;
    run_object_storage_server(addr, port);
    Py_INCREF(Py_None);
    return Py_None;
}

static PyMethodDef object_storage_server_methods[] = {
    {
        "run_object_storage_server",
        object_storage_server_run_object_storage_server,
        METH_VARARGS,
        "Run object storage server on address:port",
    },

    {NULL, NULL, 0, NULL},
};

static struct PyModuleDef object_storage_server_module = {
    //
    .m_methods = object_storage_server_methods,
    //
};

PyMODINIT_FUNC PyInit_object_storage_server(void) {
    return PyModuleDef_Init(&object_storage_server_module);
}
}

#include "scaler/object_storage/object_storage_server.h"
#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <string>

extern "C" {
static PyObject* run_object_storage_server(PyObject* self, PyObject* args) {
    const char* addr;
    int port;
    int on_server_ready_fd = -1;

    if (!PyArg_ParseTuple(args, "si|i", &addr, &port, &on_server_ready_fd))
        return NULL;
    scaler::object_storage::ObjectStorageServer server(on_server_ready_fd);
    server.run(addr, std::to_string(port));
    Py_INCREF(Py_None);
    return Py_None;
}

static PyMethodDef object_storage_server_methods[] = {
    {
        "run_object_storage_server",
        run_object_storage_server,
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

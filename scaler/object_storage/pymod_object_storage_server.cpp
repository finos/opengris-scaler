#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "scaler/object_storage/object_storage_server.h"

extern "C" {
struct PyObjectStorageServer {
    PyObject_HEAD scaler::object_storage::ObjectStorageServer server;
};

static PyObject* PyObjectStorageServerNew(
    PyTypeObject* type, [[maybe_unused]] PyObject* args, [[maybe_unused]] PyObject* kwargs)
{
    PyObjectStorageServer* self;
    self = (PyObjectStorageServer*)type->tp_alloc(type, 0);
    return (PyObject*)self;
}

static int PyObjectStorageServerInit(PyObject* self, [[maybe_unused]] PyObject* args, [[maybe_unused]] PyObject* kwargs)
{
    new (&((PyObjectStorageServer*)self)->server) scaler::object_storage::ObjectStorageServer();
    return 0;
}

static void PyObjectStorageServerDealloc(PyObject* self)
{
    ((PyObjectStorageServer*)self)->server.~ObjectStorageServer();
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject* PyObjectStorageServerRun(PyObject* self, PyObject* args)
{
    const char* addr;
    int port;
    const char* log_level;
    const char* log_format;
    const char* logging_path;

    if (!PyArg_ParseTuple(args, "sisss", &addr, &port, &log_level, &log_format, &logging_path))
        return NULL;

    ((PyObjectStorageServer*)self)->server.run(addr, std::to_string(port), log_level, log_format, logging_path);

    Py_RETURN_NONE;
}

static PyObject* PyObjectStorageServerWaitUntilReady(PyObject* self, [[maybe_unused]] PyObject* args)
{
    ((PyObjectStorageServer*)self)->server.waitUntilReady();
    Py_RETURN_NONE;
}

static PyMethodDef PyObjectStorageServerMethods[] = {
    {"run", PyObjectStorageServerRun, METH_VARARGS, "Run object storage server on address:port with logging config"},
    {"wait_until_ready", PyObjectStorageServerWaitUntilReady, METH_NOARGS, "Wait until the server is ready"},
    {NULL, NULL, 0, NULL},
};

static PyTypeObject PyObjectStorageServerType = {
    .tp_name      = "object_storage_server.ObjectStorageServer",
    .tp_basicsize = sizeof(PyObjectStorageServer),
    .tp_dealloc   = (destructor)PyObjectStorageServerDealloc,
    .tp_flags     = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .tp_doc       = "ObjectStorageServer",
    .tp_methods   = PyObjectStorageServerMethods,
    .tp_init      = (initproc)PyObjectStorageServerInit,
    .tp_new       = PyObjectStorageServerNew,
};

static PyModuleDef PyObjectStorageServerModule = {
    .m_base = PyModuleDef_HEAD_INIT,
    .m_name = "object_storage_server",
    .m_size = -1,
};

PyMODINIT_FUNC PyInit_object_storage_server(void)
{
    PyObject* m;
    if (PyType_Ready(&PyObjectStorageServerType) < 0)
        return NULL;

    m = PyModule_Create(&PyObjectStorageServerModule);
    if (m == NULL)
        return NULL;

    Py_INCREF(&PyObjectStorageServerType);
    PyModule_AddObject(m, "ObjectStorageServer", (PyObject*)&PyObjectStorageServerType);
    return m;
}
}

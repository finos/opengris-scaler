#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <pyerrors.h>

#include "scaler/object_storage/object_storage_server.h"
#include "scaler/utility/pymod/gil.h"

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
    const char* identity;
    const char* log_level;
    const char* log_format;
    PyObject* logging_paths_tuple = nullptr;
    const char* tls_cert          = nullptr;
    const char* tls_key           = nullptr;
    PyObject* on_ready            = Py_None;

    if (!PyArg_ParseTuple(
            args,
            "ssssO!|zzO",
            &addr,
            &identity,
            &log_level,
            &log_format,
            &PyTuple_Type,
            &logging_paths_tuple,
            &tls_cert,
            &tls_key,
            &on_ready))
        return nullptr;

    if (on_ready != Py_None && !PyCallable_Check(on_ready)) {
        PyErr_SetString(PyExc_TypeError, "on_ready must be callable or None");
        return nullptr;
    }

    std::vector<std::string> logging_paths;
    Py_ssize_t num_paths = PyTuple_Size(logging_paths_tuple);
    for (Py_ssize_t i = 0; i < num_paths; ++i) {
        PyObject* path_obj = PyTuple_GetItem(logging_paths_tuple, i);
        if (!PyUnicode_Check(path_obj)) {
            PyErr_SetString(PyExc_TypeError, "logging_paths must be a tuple of strings");
            return nullptr;
        }
        logging_paths.push_back(PyUnicode_AsUTF8(path_obj));
    }

    int res {};
    bool onReadyFailed {false};
    auto running = [&res, &onReadyFailed] -> bool {
        if (onReadyFailed) {
            return false;
        }
        scaler::utility::pymod::AcquireGIL gil;
        (void)gil;
        res = PyErr_CheckSignals();
        return res == 0;
    };

    // a failing callback stops the server, and run raises its exception
    auto onReady = [on_ready, &onReadyFailed](const scaler::ymq::Address& boundAddress) {
        if (on_ready == Py_None) {
            return;
        }
        scaler::utility::pymod::AcquireGIL gil;
        (void)gil;
        auto addressString = boundAddress.toString();
        if (!addressString) {
            PyErr_SetString(PyExc_RuntimeError, "ObjectStorageServer: cannot print the bound address");
            onReadyFailed = true;
            return;
        }
        PyObject* result = PyObject_CallFunction(on_ready, "s", addressString->c_str());
        if (result == nullptr) {
            onReadyFailed = true;
            return;
        }
        Py_DECREF(result);
    };

    // we have to copy this memory before releasing the GIL
    // because it's owned by Python
    std::string addressString(addr);
    std::string identityString(identity);
    std::string logLevelString(log_level);
    std::string logFormatString(log_format);

    // Build a TLSConfig only when both cert chain and private key paths are provided and non-empty.
    std::optional<scaler::ymq::TLSConfig> tlsConfig;
    if (tls_cert != nullptr && tls_cert[0] != '\0' && tls_key != nullptr && tls_key[0] != '\0') {
        tlsConfig.emplace(std::string(tls_cert), std::string(tls_key));
    }

    Py_BEGIN_ALLOW_THREADS;
    ((PyObjectStorageServer*)self)
        ->server.run(
            std::move(addressString),
            std::move(identityString),
            std::move(logLevelString),
            std::move(logFormatString),
            std::move(logging_paths),
            std::move(running),
            std::move(tlsConfig),
            std::move(onReady));
    Py_END_ALLOW_THREADS;

    if (!res && !onReadyFailed) {
        Py_RETURN_NONE;
    } else {
        return nullptr;
    }
}

static PyMethodDef PyObjectStorageServerMethods[] = {
    {"run",
     PyObjectStorageServerRun,
     METH_VARARGS,
     "Run object storage server on address:port, calling on_ready with the bound address"},
    {nullptr, nullptr, 0, nullptr},
};

static PyType_Slot PyObjectStorageServerSlots[] = {
    {Py_tp_new, (void*)PyObjectStorageServerNew},
    {Py_tp_init, (void*)PyObjectStorageServerInit},
    {Py_tp_dealloc, (void*)PyObjectStorageServerDealloc},
    {Py_tp_methods, (void*)PyObjectStorageServerMethods},
    {Py_tp_doc, (void*)"ObjectStorageServer"},
    {0, nullptr},
};

static PyType_Spec PyObjectStorageServerSpec = {
    .name      = "object_storage_server.ObjectStorageServer",
    .basicsize = sizeof(PyObjectStorageServer),
    .itemsize  = 0,
    .flags     = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .slots     = PyObjectStorageServerSlots,
};

static PyModuleDef PyObjectStorageServerModule = {
    .m_base     = PyModuleDef_HEAD_INIT,
    .m_name     = "object_storage_server",
    .m_doc      = nullptr,
    .m_size     = -1,
    .m_methods  = nullptr,
    .m_slots    = nullptr,
    .m_traverse = nullptr,
    .m_clear    = nullptr,
    .m_free     = nullptr,
};

PyMODINIT_FUNC PyInit_object_storage_server(void)
{
    PyObject* m = PyModule_Create(&PyObjectStorageServerModule);
    if (!m) {
        return nullptr;
    }

    PyObject* type = PyType_FromSpec(&PyObjectStorageServerSpec);
    if (!type) {
        Py_DECREF(m);
        return nullptr;
    }

    if (PyModule_AddObject(m, "ObjectStorageServer", type) < 0) {
        Py_DECREF(type);
        Py_DECREF(m);
        return nullptr;
    }

    return m;
}
}

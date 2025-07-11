#pragma once

// Python
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>

struct YmqState {
    PyObject* enumModule;     // Reference to the enum module
    PyObject* asyncioModule;  // Reference to the asyncio module

    PyObject* ioSocketTypeEnum;  // Reference to the IOSocketType enum
    PyObject* errorEnum;         // Reference to the Error enum
    PyObject* PyBytesYmqType;    // Reference to the BytesYmq type
    PyObject* PyMessageType;     // Reference to the Message type
    PyObject* PyIOSocketType;    // Reference to the IOSocket type
    PyObject* PyIOContextType;   // Reference to the IOContext type
    PyObject* ExceptionType;     // Reference to the Exception type
    PyObject* AwaitableType;     // Reference to the Awaitable type
};

// C++
#include <functional>
#include <string>
#include <utility>

// this function must be called from a C++ thread
// it locks the GIL and completes a future
static void future_set_result(PyObject* future, std::function<PyObject*()> fn) {
    PyGILState_STATE gstate = PyGILState_Ensure();
    // begin python critical section

    {
        PyObject* loop = PyObject_CallMethod(future, "get_loop", nullptr);

        if (!loop) {
            PyErr_SetString(PyExc_RuntimeError, "Failed to get future's loop");
            Py_DECREF(future);

            // end python critical section
            PyGILState_Release(gstate);
            return;
        }

        PyObject* set_result = PyObject_GetAttrString(future, "set_result");

        if (!set_result) {
            PyErr_SetString(PyExc_RuntimeError, "Failed to get future's set_result() method");
            Py_DECREF(future);

            // end python critical section
            PyGILState_Release(gstate);
            return;
        }

        Py_INCREF(Py_None);
        PyObject_CallMethod(loop, "call_soon_threadsafe", "OO", set_result, fn());
    }

    Py_DECREF(future);

    // end python critical section
    PyGILState_Release(gstate);
}

// First-Party
#include "scaler/io/ymq/pymod_ymq/async.h"
#include "scaler/io/ymq/pymod_ymq/bytes.h"
#include "scaler/io/ymq/pymod_ymq/exception.h"
#include "scaler/io/ymq/pymod_ymq/io_context.h"
#include "scaler/io/ymq/pymod_ymq/io_socket.h"
#include "scaler/io/ymq/pymod_ymq/message.h"

extern "C" {

static void ymq_free(YmqState* state) {
    Py_XDECREF(state->enumModule);
    Py_XDECREF(state->asyncioModule);
    Py_XDECREF(state->ioSocketTypeEnum);
    Py_XDECREF(state->PyBytesYmqType);
    Py_XDECREF(state->PyMessageType);
    Py_XDECREF(state->PyIOSocketType);
    Py_XDECREF(state->PyIOContextType);
    Py_XDECREF(state->ExceptionType);
    Py_XDECREF(state->AwaitableType);

    state->asyncioModule    = nullptr;
    state->enumModule       = nullptr;
    state->ioSocketTypeEnum = nullptr;
    state->PyBytesYmqType   = nullptr;
    state->PyMessageType    = nullptr;
    state->PyIOSocketType   = nullptr;
    state->PyIOContextType  = nullptr;
    state->ExceptionType    = nullptr;
    state->AwaitableType    = nullptr;
}

static int ymq_createIntEnum(
    PyObject* module, PyObject** storage, std::string enumName, std::vector<std::pair<std::string, int>> entries) {
    // create a python dictionary to hold the entries
    auto enumDict = PyDict_New();
    if (!enumDict) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create enum dictionary");
        return -1;
    }

    // add each entry to the dictionary
    for (const auto& entry: entries) {
        PyObject* value = PyLong_FromLong(entry.second);
        if (!value) {
            PyErr_SetString(PyExc_RuntimeError, "Failed to create enum value");
            Py_DECREF(enumDict);
            return -1;
        }

        if (PyDict_SetItemString(enumDict, entry.first.c_str(), value) < 0) {
            Py_DECREF(value);
            Py_DECREF(enumDict);
            PyErr_SetString(PyExc_RuntimeError, "Failed to set item in enum dictionary");
            return -1;
        }
        Py_DECREF(value);
    }

    auto state = (YmqState*)PyModule_GetState(module);

    if (!state) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to get module state");
        Py_DECREF(enumDict);
        return -1;
    }

    // create our class by calling enum.IntEnum(enumName, enumDict)
    auto enumClass = PyObject_CallMethod(state->enumModule, "IntEnum", "sO", enumName.c_str(), enumDict);
    Py_DECREF(enumDict);

    if (!enumClass) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create IntEnum class");
        return -1;
    }

    *storage = enumClass;

    // add the class to the module
    // this increments the reference count of enumClass
    if (PyModule_AddObjectRef(module, enumName.c_str(), enumClass) < 0) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to add IntEnum class to module");
        Py_DECREF(enumClass);
        return -1;
    }

    return 0;
}

static int ymq_createIOSocketTypeEnum(PyObject* module, YmqState* state) {
    std::vector<std::pair<std::string, int>> ioSocketTypes = {
        {"Binder", (int)IOSocketType::Binder},
        {"Sub", (int)IOSocketType::Sub},
        {"Pub", (int)IOSocketType::Pub},
        {"Dealer", (int)IOSocketType::Dealer},
        {"Router", (int)IOSocketType::Router},
        {"Pair", (int)IOSocketType::Pair}};

    if (ymq_createIntEnum(module, &state->ioSocketTypeEnum, "IOSocketType", ioSocketTypes) < 0) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create IOSocketType enum");
        return -1;
    }

    return 0;
}

static int ymq_createErrorEnum(PyObject* module, YmqState* state) {
    std::vector<std::pair<std::string, int>> errorValues = {{"TODO", 0}};

    if (ymq_createIntEnum(module, &state->errorEnum, "Error", errorValues) < 0) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create Error enum");
        return -1;
    }

    return 0;
}

static int ymq_createType(
    PyObject* module,
    PyObject** storage,
    PyType_Spec* spec,
    const char* name,
    bool add        = true,
    PyObject* bases = nullptr) {
    *storage = PyType_FromModuleAndSpec(module, spec, bases);

    if (!*storage) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create type from spec");
        return -1;
    }

    if (add)
        if (PyModule_AddObjectRef(module, name, *storage) < 0) {
            PyErr_SetString(PyExc_RuntimeError, "Failed to add type to module");
            Py_DECREF(*storage);
            return -1;
        }

    return 0;
}

static int ymq_exec(PyObject* module) {
    auto state = (YmqState*)PyModule_GetState(module);

    if (!state) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to get module state");
        return -1;
    }

    state->enumModule = PyImport_ImportModule("enum");

    if (!state->enumModule) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to import enum module");
        return -1;
    }

    state->asyncioModule = PyImport_ImportModule("asyncio");

    if (!state->asyncioModule) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to import asyncio module");
        return -1;
    }

    if (ymq_createIOSocketTypeEnum(module, state) < 0)
        return -1;

    if (ymq_createErrorEnum(module, state) < 0)
        return -1;

    if (ymq_createType(module, &state->PyBytesYmqType, &PyBytesYmq_spec, "Bytes") < 0)
        return -1;

    if (ymq_createType(module, &state->PyMessageType, &PyMessage_spec, "Message") < 0)
        return -1;

    if (ymq_createType(module, &state->PyIOSocketType, &PyIOSocket_spec, "IOSocket") < 0)
        return -1;

    if (ymq_createType(module, &state->PyIOContextType, &PyIOContext_spec, "IOContext") < 0)
        return -1;

    if (ymq_createType(module, &state->ExceptionType, &YmqException_spec, "YmqException", true, PyExc_Exception) < 0)
        return -1;

    if (ymq_createType(module, &state->AwaitableType, &Awaitable_spec, "Awaitable", false) < 0)
        return -1;

    return 0;
}
}

static PyModuleDef_Slot ymq_slots[] = {{Py_mod_exec, (void*)ymq_exec}, {0, NULL}};

static PyModuleDef ymq_module = {
    .m_base  = PyModuleDef_HEAD_INIT,
    .m_name  = "ymq",
    .m_doc   = PyDoc_STR("YMQ Python bindings"),
    .m_size  = sizeof(YmqState),
    .m_slots = ymq_slots,
    .m_free  = (freefunc)ymq_free,
};

PyMODINIT_FUNC PyInit_ymq(void);

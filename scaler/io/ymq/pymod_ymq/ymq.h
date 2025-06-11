#pragma once

// Python
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>

struct YmqState {
    PyObject* enumModule;       // Reference to the enum module
    PyObject* ioSocketTypeEnum;  // Reference to the IOSocketType enum
    PyObject* PyBytesYmqType;  // Reference to the BytesYmq type
    PyObject* PyMessageType;  // Reference to the Message type
    PyObject* PyIOSocketType;  // Reference to the IOSocket type
    PyObject* PyIOContextType;  // Reference to the IOContext type
};

// C++
#include <string>
#include <utility>

// First-Party
#include "scaler/io/ymq/io_socket.h"
#include "scaler/io/ymq/pymod_ymq/bytes.h"
#include "scaler/io/ymq/pymod_ymq/io_context.h"
#include "scaler/io/ymq/pymod_ymq/io_socket.h"
#include "scaler/io/ymq/pymod_ymq/message.h"

extern "C" {

static void ymq_free(YmqState* state) {
    Py_XDECREF(state->enumModule);
    Py_XDECREF(state->ioSocketTypeEnum);
    Py_XDECREF(state->PyBytesYmqType);
    Py_XDECREF(state->PyMessageType);
    Py_XDECREF(state->PyIOSocketType);
    Py_XDECREF(state->PyIOContextType);

    state->enumModule = nullptr;
    state->ioSocketTypeEnum = nullptr;
    state->PyBytesYmqType = nullptr;
    state->PyMessageType = nullptr;
    state->PyIOSocketType = nullptr;
    state->PyIOContextType = nullptr;
}

static int ymq_createIntEnum(PyObject* module, std::string enumName, std::vector<std::pair<std::string, int>> entries) {
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
    auto ioSocketTypeEnum = PyObject_CallMethod(state->enumModule, "IntEnum", "sO", enumName.c_str(), enumDict);
    Py_DECREF(enumDict);

    if (!ioSocketTypeEnum) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create IntEnum class");
        return -1;
    }

    state->ioSocketTypeEnum = ioSocketTypeEnum;

    // add the class to the module
    // this increments the reference count of enumClass
    if (PyModule_AddObjectRef(module, enumName.c_str(), ioSocketTypeEnum) < 0) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to add IntEnum class to module");
        Py_DECREF(ioSocketTypeEnum);
        return -1;
    }

    return 0;
}

static int ymq_createIOSocketTypeEnum(PyObject* module) {
    std::vector<std::pair<std::string, int>> ioSocketTypes = {
        {"Binder", (int)IOSocketType::Binder},
        {"Sub", (int)IOSocketType::Sub},
        {"Pub", (int)IOSocketType::Pub},
        {"Dealer", (int)IOSocketType::Dealer},
        {"Router", (int)IOSocketType::Router},
        {"Pair", (int)IOSocketType::Pair}};

    if (ymq_createIntEnum(module, "IOSocketType", ioSocketTypes) < 0) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create IOSocketType enum");
        return -1;
    }

    return 0;
}

static int ymq_createType(PyObject* module, PyObject** storage, PyType_Spec* spec, const char* name) {
    *storage = PyType_FromModuleAndSpec(module, spec, nullptr);

    if (!*storage) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create type from spec");
        return -1;
    }

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

    if (ymq_createIOSocketTypeEnum(module) < 0)
        return -1;

    if (ymq_createType(module, &state->PyBytesYmqType, &PyBytesYmq_spec, "Bytes") < 0)
        return -1;

    if (ymq_createType(module, &state->PyMessageType, &PyMessage_spec, "Message") < 0)
        return -1;

    if (ymq_createType(module, &state->PyIOSocketType, &PyIOSocket_spec, "IOSocket") < 0)
        return -1;

    if (ymq_createType(module, &state->PyIOContextType, &PyIOContext_spec, "IOContext") < 0)
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

#pragma once

// Python
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>

// C++
#include <format>
#include <functional>
#include <string>
#include <string_view>
#include <utility>

// First-party
#include "scaler/io/ymq/error.h"

struct YMQState {
    PyObject* enumModule;     // Reference to the enum module
    PyObject* asyncioModule;  // Reference to the asyncio module

    PyObject* PyIOSocketEnumType;  // Reference to the IOSocketType enum
    PyObject* PyErrorCodeType;     // Reference to the Error enum
    PyObject* PyBytesYMQType;      // Reference to the PyBytesYMQ type
    PyObject* PyMessageType;       // Reference to the Message type
    PyObject* PyIOSocketType;      // Reference to the IOSocket type
    PyObject* PyIOContextType;     // Reference to the IOContext type
    PyObject* PyExceptionType;     // Reference to the Exception type
    PyObject* PyAwaitableType;     // Reference to the Awaitable type
};

// this function must be called from a C++ thread
// this function will lock the GIL, call `fn()` and use its return value to set the future's result/exception
static void future_do(PyObject* future, const std::function<PyObject*()>& fn, const char* future_method)
{
    PyGILState_STATE gstate = PyGILState_Ensure();
    // begin python critical section

    {
        PyObject* loop = PyObject_CallMethod(future, "get_loop", nullptr);
        if (!loop) {
            Py_DECREF(future);
            PyErr_WriteUnraisable(nullptr);

            // end python critical section
            PyGILState_Release(gstate);
            return;
        }

        // if future is already done, no need to call the method
        if (PyObject_CallMethod(future, "done", nullptr) == Py_True) {
            Py_DECREF(future);
            Py_DECREF(loop);
            PyGILState_Release(gstate);
            return;
        }

        PyObject* method = PyObject_GetAttrString(future, future_method);

        // correlates with the creation of the future in `async_wrapper()`
        Py_DECREF(future);

        if (!method) {
            Py_DECREF(loop);
            Py_DECREF(method);
            PyErr_WriteUnraisable(nullptr);

            // end python critical section
            PyGILState_Release(gstate);
            return;
        }

        auto obj = PyObject_GetAttrString(loop, "call_soon_threadsafe");
        Py_DECREF(loop);

        // auto result = PyObject_CallMethod(loop, "call_soon_threadsafe", "OO", method, fn());
        auto result = PyObject_CallFunctionObjArgs(obj, method, fn(), nullptr);
        Py_DECREF(method);
        Py_DECREF(obj);

        if (!result) {
            PyErr_WriteUnraisable(nullptr);

            // end python critical section
            PyGILState_Release(gstate);
            return;
        }

        Py_DECREF(result);
    }

    // end python critical section
    PyGILState_Release(gstate);
}

static void future_set_result(PyObject* future, std::function<PyObject*()> fn)
{
    return future_do(future, fn, "set_result");
}

static void future_raise_exception(PyObject* future, std::function<PyObject*()> fn)
{
    return future_do(future, fn, "set_exception");
}

static YMQState* YMQStateFromSelf(PyObject* self)
{
    // replace with PyType_GetModuleByDef(Py_TYPE(self), &YMQ_module) in a newer Python version
    // https://docs.python.org/3/c-api/type.html#c.PyType_GetModuleByDef
    PyObject* pyModule = PyType_GetModule(Py_TYPE(self));
    if (!pyModule)
        return nullptr;

    return (YMQState*)PyModule_GetState(pyModule);
}

PyObject* PyErr_CreateFromString(PyObject* type, const char* message)
{
    auto args = Py_BuildValue("(s)", message);
    if (!args)
        return nullptr;

    PyObject* exc = PyObject_CallObject(type, args);
    Py_DECREF(args);

    return exc;
}

// First-Party
#include "scaler/io/ymq/pymod_ymq/async.h"
#include "scaler/io/ymq/pymod_ymq/bytes.h"
#include "scaler/io/ymq/pymod_ymq/exception.h"
#include "scaler/io/ymq/pymod_ymq/io_context.h"
#include "scaler/io/ymq/pymod_ymq/io_socket.h"
#include "scaler/io/ymq/pymod_ymq/message.h"

extern "C" {

static void YMQ_free(YMQState* state)
{
    Py_XDECREF(state->enumModule);
    Py_XDECREF(state->asyncioModule);
    Py_XDECREF(state->PyIOSocketEnumType);
    Py_XDECREF(state->PyBytesYMQType);
    Py_XDECREF(state->PyMessageType);
    Py_XDECREF(state->PyIOSocketType);
    Py_XDECREF(state->PyIOContextType);
    Py_XDECREF(state->PyExceptionType);
    Py_XDECREF(state->PyAwaitableType);

    state->asyncioModule      = nullptr;
    state->enumModule         = nullptr;
    state->PyIOSocketEnumType = nullptr;
    state->PyBytesYMQType     = nullptr;
    state->PyMessageType      = nullptr;
    state->PyIOSocketType     = nullptr;
    state->PyIOContextType    = nullptr;
    state->PyExceptionType    = nullptr;
    state->PyAwaitableType    = nullptr;
}

static int YMQ_createIntEnum(
    PyObject* pyModule, PyObject** storage, std::string enumName, std::vector<std::pair<std::string, int>> entries)
{
    // create a python dictionary to hold the entries
    auto enumDict = PyDict_New();
    if (!enumDict)
        return -1;

    // add each entry to the dictionary
    for (const auto& entry: entries) {
        PyObject* value = PyLong_FromLong(entry.second);
        if (!value) {
            Py_DECREF(enumDict);
            return -1;
        }

        auto status = PyDict_SetItemString(enumDict, entry.first.c_str(), value);
        Py_DECREF(value);

        if (status < 0) {
            Py_DECREF(enumDict);
            return -1;
        }
    }

    auto state = (YMQState*)PyModule_GetState(pyModule);

    if (!state) {
        Py_DECREF(enumDict);
        return -1;
    }

    // create our class by calling enum.IntEnum(enumName, enumDict)
    auto enumClass = PyObject_CallMethod(state->enumModule, "IntEnum", "sO", enumName.c_str(), enumDict);
    Py_DECREF(enumDict);

    if (!enumClass)
        return -1;

    *storage = enumClass;

    // add the class to the module
    // this increments the reference count of enumClass
    auto status = PyModule_AddObjectRef(pyModule, enumName.c_str(), enumClass);
    Py_DECREF(enumClass);

    return status;
}

static int YMQ_createIOSocketTypeEnum(PyObject* pyModule, YMQState* state)
{
    std::vector<std::pair<std::string, int>> ioSocketTypes = {
        {"Uninit", (int)IOSocketType::Uninit},
        {"Binder", (int)IOSocketType::Binder},
        {"Connector", (int)IOSocketType::Connector},
        {"Unicast", (int)IOSocketType::Unicast},
        {"Multicast", (int)IOSocketType::Multicast},
    };

    return YMQ_createIntEnum(pyModule, &state->PyIOSocketEnumType, "IOSocketType", ioSocketTypes);
}

static PyObject* YMQErrorCode_explanation(PyObject* self, PyObject* Py_UNUSED(args))
{
    auto pyValue = PyObject_GetAttrString(self, "value");
    if (!pyValue)
        return nullptr;

    if (!PyLong_Check(pyValue)) {
        PyErr_SetString(PyExc_TypeError, "Expected an integer value");
        Py_DECREF(pyValue);
        return nullptr;
    }

    long value = PyLong_AsLong(pyValue);
    Py_DECREF(pyValue);

    if (value == -1 && PyErr_Occurred())
        return nullptr;

    std::string_view explanation = Error::convertErrorToExplanation(static_cast<Error::ErrorCode>(value));
    return PyUnicode_FromString(std::string(explanation).c_str());
}

// IDEA: CREATE AN INT ENUM AND ATTACH METHOD AFTERWARDS
// OR: CREATE A NON-INT ENUM AND USE A TUPLE FOR THE VALUES
static int YMQ_createErrorCodeEnum(PyObject* pyModule, YMQState* state)
{
    std::vector<std::pair<std::string, int>> errorCodeValues = {
        {"Uninit", (int)Error::ErrorCode::Uninit},
        {"InvalidPortFormat", (int)Error::ErrorCode::InvalidPortFormat},
        {"InvalidAddressFormat", (int)Error::ErrorCode::InvalidAddressFormat},
        {"ConfigurationError", (int)Error::ErrorCode::ConfigurationError},
        {"SignalNotSupported", (int)Error::ErrorCode::SignalNotSupported},
        {"CoreBug", (int)Error::ErrorCode::CoreBug},
        {"RepetetiveIOSocketIdentity", (int)Error::ErrorCode::RepetetiveIOSocketIdentity},
        {"RedundantIOSocketRefCount", (int)Error::ErrorCode::RedundantIOSocketRefCount},
        {"MultipleConnectToNotSupported", (int)Error::ErrorCode::MultipleConnectToNotSupported},
        {"MultipleBindToNotSupported", (int)Error::ErrorCode::MultipleBindToNotSupported},
        {"InitialConnectFailedWithInProgress", (int)Error::ErrorCode::InitialConnectFailedWithInProgress},
        {"SendMessageRequestCouldNotComplete", (int)Error::ErrorCode::SendMessageRequestCouldNotComplete},
        {"SetSockOptNonFatalFailure", (int)Error::ErrorCode::SetSockOptNonFatalFailure},
        {"IPv6NotSupported", (int)Error::ErrorCode::IPv6NotSupported},
        {"RemoteEndDisconnectedOnSocketWithoutGuaranteedDelivery",
         (int)Error::ErrorCode::RemoteEndDisconnectedOnSocketWithoutGuaranteedDelivery},
    };

    if (YMQ_createIntEnum(pyModule, &state->PyErrorCodeType, "ErrorCode", errorCodeValues) < 0)
        return -1;

    static PyMethodDef YMQErrorCode_explanation_def = {
        "explanation",
        (PyCFunction)YMQErrorCode_explanation,
        METH_NOARGS,
        PyDoc_STR("Returns an explanation of a YMQ error code")};

    auto iter = PyObject_GetIter(state->PyErrorCodeType);
    if (!iter)
        return -1;

    // is this the best way to add a method to each enum item?
    // in python you can just write: MyEnum.new_method = ...
    // for some reason this does not seem to work with the c api
    // docs and examples are unfortunately scarce for this
    // for now this will work just fine
    PyObject* item = nullptr;
    while ((item = PyIter_Next(iter)) != nullptr) {
        auto fn = PyCMethod_New(&YMQErrorCode_explanation_def, item, pyModule, nullptr);
        if (!fn)
            return -1;

        auto status = PyObject_SetAttrString(item, "explanation", fn);
        Py_DECREF(item);
        Py_DECREF(fn);

        if (status < 0) {
            Py_DECREF(iter);
            return -1;
        }
    }

    Py_DECREF(iter);
    return 0;
}
}

// internal convenience function to create a type and add it to the module
static int YMQ_createType(
    // the module object
    PyObject* pyModule,
    // storage for the generated type object
    PyObject** storage,
    // the type's spec
    PyType_Spec* spec,
    // the name of the type, can be omitted if `add` is false
    const char* name,
    // whether or not to add this type to the module
    bool add = true,
    // the types base classes
    PyObject* bases = nullptr)
{
    assert(storage != nullptr);

    *storage = PyType_FromModuleAndSpec(pyModule, spec, bases);
    if (!*storage)
        return -1;

    if (add)
        if (PyModule_AddObjectRef(pyModule, name, *storage) < 0)
            return -1;

    return 0;
}

static int YMQ_exec(PyObject* pyModule)
{
    auto state = (YMQState*)PyModule_GetState(pyModule);
    if (!state)
        return -1;

    state->enumModule = PyImport_ImportModule("enum");
    if (!state->enumModule)
        return -1;

    state->asyncioModule = PyImport_ImportModule("asyncio");
    if (!state->asyncioModule)
        return -1;

    if (YMQ_createIOSocketTypeEnum(pyModule, state) < 0)
        return -1;

    if (YMQ_createErrorCodeEnum(pyModule, state) < 0)
        return -1;

    if (YMQ_createType(pyModule, &state->PyBytesYMQType, &PyBytesYMQ_spec, "Bytes") < 0)
        return -1;

    if (YMQ_createType(pyModule, &state->PyMessageType, &PyMessage_spec, "Message") < 0)
        return -1;

    if (YMQ_createType(pyModule, &state->PyIOSocketType, &PyIOSocket_spec, "IOSocket") < 0)
        return -1;

    if (YMQ_createType(pyModule, &state->PyIOContextType, &PyIOContext_spec, "IOContext") < 0)
        return -1;

    if (YMQ_createType(pyModule, &state->PyExceptionType, &YMQException_spec, "YMQException", true, PyExc_Exception) <
        0)
        return -1;

    if (YMQ_createType(pyModule, &state->PyAwaitableType, &Awaitable_spec, "Awaitable", false) < 0)
        return -1;

    return 0;
}

static PyModuleDef_Slot YMQ_slots[] = {
    {Py_mod_exec, (void*)YMQ_exec},
    {0, nullptr},
};

static PyModuleDef YMQ_module = {
    .m_base  = PyModuleDef_HEAD_INIT,
    .m_name  = "ymq",
    .m_doc   = PyDoc_STR("YMQ Python bindings"),
    .m_size  = sizeof(YMQState),
    .m_slots = YMQ_slots,
    .m_free  = (freefunc)YMQ_free,
};

PyMODINIT_FUNC PyInit_ymq(void);

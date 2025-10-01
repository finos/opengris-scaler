#pragma once

// Python
#include "scaler/io/ymq/pymod_ymq/python.h"

// C
#include <fcntl.h>
#include <unistd.h>

// C++
#include <expected>
#include <format>
#include <functional>
#include <string>
#include <string_view>
#include <utility>

// First-party
#include "scaler/io/ymq/error.h"
#include "scaler/io/ymq/pymod_ymq/utils.h"

struct YMQState {
    int wakeupfd_wr;
    int wakeupfd_rd;

    OwnedPyObject<> enumModule;     // Reference to the enum module
    OwnedPyObject<> asyncioModule;  // Reference to the asyncio module

    OwnedPyObject<> PyIOSocketEnumType;          // Reference to the IOSocketType enum
    OwnedPyObject<> PyErrorCodeType;             // Reference to the Error enum
    OwnedPyObject<> PyBytesYMQType;              // Reference to the PyBytesYMQ type
    OwnedPyObject<> PyMessageType;               // Reference to the Message type
    OwnedPyObject<> PyIOSocketType;              // Reference to the IOSocket type
    OwnedPyObject<> PyIOContextType;             // Reference to the IOContext type
    OwnedPyObject<> PyExceptionType;             // Reference to the Exception type
    OwnedPyObject<> PyInterruptedExceptionType;  // Reference to the YMQInterruptedException type
    OwnedPyObject<> PyTimeoutExceptionType;      // Reference to the YMQTimeoutException type
    OwnedPyObject<> PyAwaitableType;             // Reference to the Awaitable type
};

#define WAIT(waiter)                                                                                                \
    do {                                                                                                            \
        auto result = waiter.wait();                                                                                \
        if (result == WaitResult::Signal) {                                                                         \
            PyEval_RestoreThread(_save);                                                                            \
            if (PyErr_CheckSignals() >= 0)                                                                          \
                PyErr_SetString(                                                                                    \
                    *state->PyInterruptedExceptionType, "A synchronous YMQ operation was interrupted by a signal"); \
            return (PyObject*)nullptr;                                                                              \
        } else if (result == WaitResult::Timeout) {                                                                 \
            PyEval_RestoreThread(_save);                                                                            \
            PyErr_SetString(*state->PyTimeoutExceptionType, "A synchronous YMQ operation timed out");               \
            return (PyObject*)nullptr;                                                                              \
        }                                                                                                           \
    } while (0);

static bool future_do_(PyObject* future_, const std::function<std::expected<PyObject*, PyObject*>()>& fn)
{
    // this is an owned reference to the future created in `async_wrapper()`
    OwnedPyObject future(future_);
    OwnedPyObject loop = PyObject_CallMethod(*future, "get_loop", nullptr);
    if (!loop)
        return true;

    // if future is already done, no need to call the method
    OwnedPyObject result1 = PyObject_CallMethod(*future, "done", nullptr);
    if (*result1 == Py_True)
        return false;

    const char* method_name = nullptr;
    OwnedPyObject arg {};

    if (auto result = fn()) {
        method_name = "set_result";
        arg         = *result;
    } else {
        method_name = "set_exception";
        arg         = result.error();
    }

    OwnedPyObject method = PyObject_GetAttrString(*future, method_name);
    if (!method)
        return true;

    OwnedPyObject obj = PyObject_GetAttrString(*loop, "call_soon_threadsafe");

    // auto result = PyObject_CallMethod(loop, "call_soon_threadsafe", "OO", method, fn());
    OwnedPyObject result2 = PyObject_CallFunctionObjArgs(*obj, *method, *arg, nullptr);
    return !result2;
}

// this function must be called from a C++ thread
// this function will lock the GIL, call `fn()` and use its return value to set the future's result/exception
static void future_do(PyObject* future, const std::function<std::expected<PyObject*, PyObject*>()>& fn)
{
    PyGILState_STATE gstate = PyGILState_Ensure();
    // begin python critical section

    auto error = future_do_(future, fn);
    if (error)
        PyErr_WriteUnraisable(future);

    // end python critical section
    PyGILState_Release(gstate);
}

static void future_set_result(PyObject* future, std::function<std::expected<PyObject*, PyObject*>()> fn)
{
    return future_do(future, fn);
}

static void future_raise_exception(PyObject* future, std::function<PyObject*()> fn)
{
    return future_do(future, [=] { return std::unexpected {fn()}; });
}

static YMQState* YMQStateFromSelf(PyObject* self)
{
    // replace with PyType_GetModuleByDef(Py_TYPE(self), &YMQ_module) in a newer Python version
    // https://docs.python.org/3/c-api/type.html#c.PyType_GetModuleByDef
    PyObject* pyModule = PyType_GetModule(Py_TYPE(self));
    if (!pyModule)
        return nullptr;

#if PY_MAJOR_VERSION == 3 && PY_MINOR_VERSION == 8
    Py_DECREF(pyModule);  // As we get a real ref in 3.8 backport
#endif

    return (YMQState*)PyModule_GetState(pyModule);
}

PyObject* PyErr_CreateFromString(PyObject* type, const char* message)
{
    OwnedPyObject args = Py_BuildValue("(s)", message);
    if (!args)
        return nullptr;

    return PyObject_CallObject(type, *args);
}

// this is a polyfill for PyErr_GetRaisedException() added in Python 3.12+
std::expected<PyObject*, PyObject*> YMQ_GetRaisedException()
{
#if (PY_MAJOR_VERSION <= 3) && (PY_MINOR_VERSION <= 12)
    PyObject *excType, *excValue, *excTraceback;
    PyErr_Fetch(&excType, &excValue, &excTraceback);
    Py_XDECREF(excType);
    Py_XDECREF(excTraceback);
    if (!excValue)
        Py_RETURN_NONE;

    return std::unexpected {excValue};
#else
    PyObject* excValue = PyErr_GetRaisedException();
    if (!excValue)
        Py_RETURN_NONE;

    return std::unexpected {excValue};
#endif
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
    try {
        state->enumModule.~OwnedPyObject();
        state->asyncioModule.~OwnedPyObject();
        state->PyIOSocketEnumType.~OwnedPyObject();
        state->PyErrorCodeType.~OwnedPyObject();
        state->PyBytesYMQType.~OwnedPyObject();
        state->PyMessageType.~OwnedPyObject();
        state->PyIOSocketType.~OwnedPyObject();
        state->PyIOContextType.~OwnedPyObject();
        state->PyExceptionType.~OwnedPyObject();
        state->PyInterruptedExceptionType.~OwnedPyObject();
        state->PyTimeoutExceptionType.~OwnedPyObject();
        state->PyAwaitableType.~OwnedPyObject();
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to free YMQState");
        PyErr_WriteUnraisable(nullptr);
    }

    if (close(state->wakeupfd_wr) < 0) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to close waitfd_wr");
        PyErr_WriteUnraisable(nullptr);
    }

    if (close(state->wakeupfd_rd) < 0) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to close waitfd_rd");
        PyErr_WriteUnraisable(nullptr);
    }
}

static int YMQ_createIntEnum(
    PyObject* pyModule,
    OwnedPyObject<>* storage,
    std::string enumName,
    std::vector<std::pair<std::string, int>> entries)
{
    // create a python dictionary to hold the entries
    OwnedPyObject enumDict = PyDict_New();
    if (!enumDict)
        return -1;

    // add each entry to the dictionary
    for (const auto& entry: entries) {
        OwnedPyObject value = PyLong_FromLong(entry.second);
        if (!value)
            return -1;

        auto status = PyDict_SetItemString(*enumDict, entry.first.c_str(), *value);
        if (status < 0)
            return -1;
    }

    auto state = (YMQState*)PyModule_GetState(pyModule);

    if (!state)
        return -1;

    // create our class by calling enum.IntEnum(enumName, enumDict)
    OwnedPyObject enumClass = PyObject_CallMethod(*state->enumModule, "IntEnum", "sO", enumName.c_str(), *enumDict);
    if (!enumClass)
        return -1;

    *storage = enumClass;

    // add the class to the module
    // this increments the reference count of enumClass
    return PyModule_AddObjectRef(pyModule, enumName.c_str(), *enumClass);
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
    OwnedPyObject pyValue = PyObject_GetAttrString(self, "value");
    if (!pyValue)
        return nullptr;

    if (!PyLong_Check(*pyValue)) {
        PyErr_SetString(PyExc_TypeError, "Expected an integer value");
        return nullptr;
    }

    long value = PyLong_AsLong(*pyValue);

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
        {"ConnectorSocketClosedByRemoteEnd", (int)Error::ErrorCode::ConnectorSocketClosedByRemoteEnd},
    };

    if (YMQ_createIntEnum(pyModule, &state->PyErrorCodeType, "ErrorCode", errorCodeValues) < 0)
        return -1;

    static PyMethodDef YMQErrorCode_explanation_def = {
        "explanation",
        (PyCFunction)YMQErrorCode_explanation,
        METH_NOARGS,
        PyDoc_STR("Returns an explanation of a YMQ error code")};

    OwnedPyObject iter = PyObject_GetIter(*state->PyErrorCodeType);
    if (!iter)
        return -1;

    // is this the best way to add a method to each enum item?
    // in python you can just write: MyEnum.new_method = ...
    // for some reason this does not seem to work with the c api
    // docs and examples are unfortunately scarce for this
    // for now this will work just fine
    OwnedPyObject item {};
    while ((item = PyIter_Next(*iter))) {
        OwnedPyObject fn = PyCFunction_NewEx(&YMQErrorCode_explanation_def, *item, pyModule);
        if (!fn)
            return -1;

        auto status = PyObject_SetAttrString(*item, "explanation", *fn);
        if (status < 0)
            return -1;
    }

    return 0;
}
}

static int YMQ_createInterruptedException(PyObject* pyModule, OwnedPyObject<>* storage)
{
    *storage = PyErr_NewExceptionWithDoc(
        "ymq.YMQInterruptedException",
        "Raised when a synchronous method is interrupted by a signal",
        PyExc_Exception,
        nullptr);

    if (!*storage)
        return -1;
    if (PyModule_AddObjectRef(pyModule, "YMQInterruptedException", **storage) < 0)
        return -1;
    return 0;
}

static int YMQ_createTimeoutException(PyObject* pyModule, OwnedPyObject<>* storage)
{
    *storage = PyErr_NewExceptionWithDoc(
        "ymq.YMQTimeoutException", "Raised when a synchronous method times out", PyExc_Exception, nullptr);

    if (!*storage)
        return -1;
    if (PyModule_AddObjectRef(pyModule, "YMQTimeoutException", **storage) < 0)
        return -1;
    return 0;
}

// internal convenience function to create a type and add it to the module
static int YMQ_createType(
    // the module object
    PyObject* pyModule,
    // storage for the generated type object
    OwnedPyObject<>* storage,
    // the type's spec
    PyType_Spec* spec,
    // the name of the type, can be omitted if `add` is false
    const char* name,
    // whether or not to add this type to the module
    bool add = true,
    // the types base classes
    PyObject* bases                 = nullptr,
    getbufferproc getbuffer         = nullptr,
    releasebufferproc releasebuffer = nullptr)
{
    assert(storage != nullptr);

    *storage = PyType_FromModuleAndSpec(pyModule, spec, bases);
    if (!*storage)
        return -1;

#if PY_MAJOR_VERSION == 3 && PY_MINOR_VERSION == 8
    if (PyObject_SetAttrString(**storage, "__module_object__", pyModule) < 0)
        return -1;

    if (getbuffer && releasebuffer) {
        PyTypeObject* type_obj = (PyTypeObject*)**storage;

        type_obj->tp_as_buffer->bf_getbuffer     = getbuffer;
        type_obj->tp_as_buffer->bf_releasebuffer = releasebuffer;
        type_obj->tp_flags |= 0;  // Do I need to add tp_flags? Seems not
    }
#endif

    if (add)
        if (PyModule_AddObjectRef(pyModule, name, **storage) < 0)
            return -1;

    return 0;
}

static int YMQ_setupWakeupFd(YMQState* state)
{
    int pipefd[2];
    if (pipe2(pipefd, O_NONBLOCK | O_CLOEXEC) < 0) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create pipe for wakeup fd");
        return -1;
    }

    state->wakeupfd_rd = pipefd[0];
    state->wakeupfd_wr = pipefd[1];

    OwnedPyObject signalModule = PyImport_ImportModule("signal");
    if (!signalModule)
        return -1;

    OwnedPyObject result = PyObject_CallMethod(*signalModule, "set_wakeup_fd", "i", state->wakeupfd_wr);
    if (!result)
        return -1;
    return 0;
}

static int YMQ_exec(PyObject* pyModule)
{
    auto state = (YMQState*)PyModule_GetState(pyModule);
    if (!state)
        return -1;

    if (YMQ_setupWakeupFd(state) < 0)
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

#if PY_MAJOR_VERSION == 3 && PY_MINOR_VERSION == 8
    if (YMQ_createType(
            pyModule,
            &state->PyBytesYMQType,
            &PyBytesYMQ_spec,
            "Bytes",
            true,
            nullptr,
            (getbufferproc)PyBytesYMQ_getbuffer,
            (releasebufferproc)PyBytesYMQ_releasebuffer) < 0)
        return -1;
#else
    if (YMQ_createType(pyModule, &state->PyBytesYMQType, &PyBytesYMQ_spec, "Bytes") < 0)
        return -1;
#endif

    if (YMQ_createType(pyModule, &state->PyMessageType, &PyMessage_spec, "Message") < 0)
        return -1;

    if (YMQ_createType(pyModule, &state->PyIOSocketType, &PyIOSocket_spec, "IOSocket") < 0)
        return -1;

    if (YMQ_createType(pyModule, &state->PyIOContextType, &PyIOContext_spec, "IOContext") < 0)
        return -1;

    PyObject* exceptionBases = PyTuple_Pack(1, PyExc_Exception);
    if (!exceptionBases)
        return -1;

    if (YMQ_createType(pyModule, &state->PyExceptionType, &YMQException_spec, "YMQException", true, exceptionBases) <
        0) {
        Py_DECREF(exceptionBases);
        return -1;
    }
    Py_DECREF(exceptionBases);

    if (YMQ_createInterruptedException(pyModule, &state->PyInterruptedExceptionType) < 0)
        return -1;

    if (YMQ_createTimeoutException(pyModule, &state->PyTimeoutExceptionType) < 0)
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

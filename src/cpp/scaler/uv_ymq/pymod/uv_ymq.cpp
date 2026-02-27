#include "scaler/uv_ymq/pymod/uv_ymq.h"

#include <initializer_list>
#include <new>
#include <string_view>
#include <utility>

#include "scaler/error/error.h"
#include "scaler/uv_ymq/configuration.h"
#include "scaler/uv_ymq/pymod/address.h"
#include "scaler/uv_ymq/pymod/binder_socket.h"
#include "scaler/uv_ymq/pymod/bytes.h"
#include "scaler/uv_ymq/pymod/connector_socket.h"
#include "scaler/uv_ymq/pymod/exception.h"
#include "scaler/uv_ymq/pymod/io_context.h"
#include "scaler/uv_ymq/pymod/message.h"

namespace scaler {
namespace uv_ymq {
namespace pymod {

UVYMQState* UVYMQStateFromSelf(PyObject* self)
{
    PyObject* pyModule = PyType_GetModule(Py_TYPE(self));
    if (!pyModule)
        return nullptr;

#if PY_MAJOR_VERSION == 3 && PY_MINOR_VERSION == 8
    Py_DECREF(pyModule);  // As we get a real ref in 3.8 backport
#endif

    return (UVYMQState*)PyModule_GetState(pyModule);
}

void UVYMQ_free(void* stateVoid)
{
    UVYMQState* state = (UVYMQState*)stateVoid;
    if (state) {
        state->~UVYMQState();
    }
}

int UVYMQ_createIntEnum(
    PyObject* pyModule,
    OwnedPyObject<>* storage,
    std::string enumName,
    std::initializer_list<std::pair<const char*, int>> entries)
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

        auto status = PyDict_SetItemString(*enumDict, entry.first, *value);
        if (status < 0)
            return -1;
    }

    auto state = (UVYMQState*)PyModule_GetState(pyModule);

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

static PyObject* UVYMQErrorCode_explanation(PyObject* self, PyObject* Py_UNUSED(args))
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

    std::string_view explanation =
        scaler::ymq::Error::convertErrorToExplanation(static_cast<scaler::ymq::Error::ErrorCode>(value));
    return PyUnicode_FromString(std::string(explanation).c_str());
}

int UVYMQ_createErrorCodeEnum(PyObject* pyModule, UVYMQState* state)
{
    using ErrorCode = scaler::ymq::Error::ErrorCode;

    const std::initializer_list<std::pair<const char*, int>> errorCodeValues = {
        {"Uninit", (int)ErrorCode::Uninit},
        {"InvalidPortFormat", (int)ErrorCode::InvalidPortFormat},
        {"InvalidAddressFormat", (int)ErrorCode::InvalidAddressFormat},
        {"ConfigurationError", (int)ErrorCode::ConfigurationError},
        {"SignalNotSupported", (int)ErrorCode::SignalNotSupported},
        {"CoreBug", (int)ErrorCode::CoreBug},
        {"RepetetiveIOSocketIdentity", (int)ErrorCode::RepetetiveIOSocketIdentity},
        {"RedundantIOSocketRefCount", (int)ErrorCode::RedundantIOSocketRefCount},
        {"MultipleConnectToNotSupported", (int)ErrorCode::MultipleConnectToNotSupported},
        {"MultipleBindToNotSupported", (int)ErrorCode::MultipleBindToNotSupported},
        {"InitialConnectFailedWithInProgress", (int)ErrorCode::InitialConnectFailedWithInProgress},
        {"SendMessageRequestCouldNotComplete", (int)ErrorCode::SendMessageRequestCouldNotComplete},
        {"SetSockOptNonFatalFailure", (int)ErrorCode::SetSockOptNonFatalFailure},
        {"IPv6NotSupported", (int)ErrorCode::IPv6NotSupported},
        {"RemoteEndDisconnectedOnSocketWithoutGuaranteedDelivery",
         (int)ErrorCode::RemoteEndDisconnectedOnSocketWithoutGuaranteedDelivery},
        {"ConnectorSocketClosedByRemoteEnd", (int)ErrorCode::ConnectorSocketClosedByRemoteEnd},
        {"IOSocketStopRequested", (int)ErrorCode::IOSocketStopRequested},
        {"BinderSendMessageWithNoAddress", (int)ErrorCode::BinderSendMessageWithNoAddress},
        {"IPCOnWinNotSupported", (int)ErrorCode::IPCOnWinNotSupported},
    };

    if (UVYMQ_createIntEnum(pyModule, &state->PyErrorCodeType, "ErrorCode", errorCodeValues) < 0)
        return -1;

    static PyMethodDef UVYMQErrorCode_explanation_def = {
        "explanation",
        (PyCFunction)UVYMQErrorCode_explanation,
        METH_NOARGS,
        PyDoc_STR("Returns an explanation of a UVYMQ error code")};

    OwnedPyObject iter = PyObject_GetIter(*state->PyErrorCodeType);
    if (!iter)
        return -1;

    OwnedPyObject item {};
    while ((item = PyIter_Next(*iter))) {
        OwnedPyObject fn = PyCFunction_NewEx(&UVYMQErrorCode_explanation_def, *item, pyModule);
        if (!fn)
            return -1;

        auto status = PyObject_SetAttrString(*item, "explanation", *fn);
        if (status < 0)
            return -1;
    }

    if (PyErr_Occurred())
        return -1;

    return 0;
}

int UVYMQ_createExceptions(PyObject* pyModule, UVYMQState* state)
{
    using ErrorCode = scaler::ymq::Error::ErrorCode;

    const std::initializer_list<std::pair<ErrorCode, std::string>> exceptions = {
        {ErrorCode::InvalidPortFormat, "InvalidPortFormatError"},
        {ErrorCode::InvalidAddressFormat, "InvalidAddressFormatError"},
        {ErrorCode::ConfigurationError, "ConfigurationError"},
        {ErrorCode::SignalNotSupported, "SignalNotSupportedError"},
        {ErrorCode::CoreBug, "CoreBugError"},
        {ErrorCode::RepetetiveIOSocketIdentity, "RepetetiveIOSocketIdentityError"},
        {ErrorCode::RedundantIOSocketRefCount, "RedundantIOSocketRefCountError"},
        {ErrorCode::MultipleConnectToNotSupported, "MultipleConnectToNotSupportedError"},
        {ErrorCode::MultipleBindToNotSupported, "MultipleBindToNotSupportedError"},
        {ErrorCode::InitialConnectFailedWithInProgress, "InitialConnectFailedWithInProgressError"},
        {ErrorCode::SendMessageRequestCouldNotComplete, "SendMessageRequestCouldNotCompleteError"},
        {ErrorCode::SetSockOptNonFatalFailure, "SetSockOptNonFatalFailureError"},
        {ErrorCode::IPv6NotSupported, "IPv6NotSupportedError"},
        {ErrorCode::RemoteEndDisconnectedOnSocketWithoutGuaranteedDelivery,
         "RemoteEndDisconnectedOnSocketWithoutGuaranteedDeliveryError"},
        {ErrorCode::ConnectorSocketClosedByRemoteEnd, "ConnectorSocketClosedByRemoteEndError"},
        {ErrorCode::IOSocketStopRequested, "IOSocketStopRequestedError"},
        {ErrorCode::BinderSendMessageWithNoAddress, "BinderSendMessageWithNoAddressError"},
        {ErrorCode::IPCOnWinNotSupported, "IPCOnWinNotSupportedError"},
        {ErrorCode::UVError, "UVError"},
    };

    static PyType_Slot slots[] = {{0, nullptr}};

    for (const auto& entry: exceptions) {
        std::string fullName = "_uv_ymq." + entry.second;

        PyType_Spec spec = {
            fullName.c_str(),
            0,
            0,
            Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
            slots,
        };

        OwnedPyObject<> bases = PyTuple_Pack(1, *state->PyExceptionType);
        if (!bases)
            return -1;

        PyObject* subtype = PyType_FromModuleAndSpec(pyModule, &spec, *bases);

        if (!subtype)
            return -1;

        state->PyExceptionSubtypes[(int)entry.first] = subtype;

        if (PyModule_AddObjectRef(pyModule, entry.second.c_str(), subtype) < 0)
            return -1;
    }

    return 0;
}

static int UVYMQ_createType(
    PyObject* pyModule,
    OwnedPyObject<>* storage,
    PyType_Spec* spec,
    const char* name,
    bool add,
    PyObject* bases,
    getbufferproc getbuffer,
    releasebufferproc releasebuffer)
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

static int UVYMQ_exec(PyObject* pyModule)
{
    auto state = (UVYMQState*)PyModule_GetState(pyModule);
    if (!state)
        return -1;

    // Use placement new to initialize C++ objects in the pre-allocated (zero-initialized) memory
    new (state) UVYMQState();

    state->enumModule = PyImport_ImportModule("enum");
    if (!state->enumModule)
        return -1;

    if (UVYMQ_createErrorCodeEnum(pyModule, state) < 0)
        return -1;

#if PY_MAJOR_VERSION == 3 && PY_MINOR_VERSION == 8
    if (UVYMQ_createType(
            pyModule,
            &state->PyBytesType,
            &PyBytes_spec,
            "Bytes",
            true,
            nullptr,
            (getbufferproc)PyBytes_getbuffer,
            (releasebufferproc)PyBytes_releasebuffer) < 0)
        return -1;
#else
    if (UVYMQ_createType(pyModule, &state->PyBytesType, &PyBytes_spec, "Bytes") < 0)
        return -1;
#endif

    if (UVYMQ_createType(pyModule, &state->PyMessageType, &PyMessage_spec, "Message") < 0)
        return -1;

    if (UVYMQ_createType(pyModule, &state->PyIOContextType, &PyIOContext_spec, "IOContext") < 0)
        return -1;

    if (PyAddressType_createEnum(pyModule, state) < 0)
        return -1;

    if (UVYMQ_createType(pyModule, &state->PyAddressType, &PyAddress_spec, "Address") < 0)
        return -1;

    if (UVYMQ_createType(pyModule, &state->PyBinderSocketType, &PyBinderSocket_spec, "BinderSocket") < 0)
        return -1;

    if (UVYMQ_createType(pyModule, &state->PyConnectorSocketType, &PyConnectorSocket_spec, "ConnectorSocket") < 0)
        return -1;

    // Add configuration constants
    if (PyModule_AddIntConstant(pyModule, "DEFAULT_MAX_RETRY_TIMES", defaultClientMaxRetryTimes) < 0)
        return -1;

    if (PyModule_AddIntConstant(pyModule, "DEFAULT_INIT_RETRY_DELAY", defaultClientInitRetryDelay.count()) < 0)
        return -1;

    PyObject* exceptionBases = PyTuple_Pack(1, PyExc_Exception);
    if (!exceptionBases)
        return -1;

    if (UVYMQ_createType(
            pyModule, &state->PyExceptionType, &UVYMQException_spec, "UVYMQException", true, exceptionBases) < 0) {
        Py_DECREF(exceptionBases);
        return -1;
    }
    Py_DECREF(exceptionBases);

    if (UVYMQ_createExceptions(pyModule, state) < 0)
        return -1;

    return 0;
}

}  // namespace pymod
}  // namespace uv_ymq
}  // namespace scaler

PyMODINIT_FUNC PyInit__uv_ymq(void)
{
    return PyModuleDef_Init(&scaler::uv_ymq::pymod::UVYMQ_module);
}

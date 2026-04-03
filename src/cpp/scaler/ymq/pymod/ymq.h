#pragma once

// C++
#include <initializer_list>
#include <string>
#include <utility>

#include "scaler/error/error.h"
#include "scaler/utility/pymod/compatibility.h"

namespace scaler {
namespace ymq {
namespace pymod {

using scaler::utility::pymod::OwnedPyObject;

struct YMQState {
    OwnedPyObject<> enumModule;  // Reference to the enum module

    OwnedPyObject<> PyIOContextType;  // Reference to the IOContext type

    OwnedPyObject<> PyBinderSocketType;     // Reference to the BinderSocket type
    OwnedPyObject<> PyConnectorSocketType;  // Reference to the ConnectorSocket type

    OwnedPyObject<> PyAddressTypeEnumType;  // Reference to the Address.Type enum
    OwnedPyObject<> PyAddressType;          // Reference to the Address type
    OwnedPyObject<> PyErrorCodeType;        // Reference to the ErrorCode enum
    OwnedPyObject<> PyBytesType;            // Reference to Bytes type
    OwnedPyObject<> PyMessageType;          // Reference to Message type
    OwnedPyObject<> PyExceptionType;        // Reference to YMQException type

    std::unordered_map<int, OwnedPyObject<>> PyExceptionSubtypes;  // Map of error code to exception subclass
};

YMQState* YMQStateFromSelf(PyObject* self);

// Like YMQStateFromSelf but for class methods where the first arg is the type (cls), not an instance.
YMQState* YMQStateFromType(PyObject* type);

void YMQ_free(void* stateVoid);

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
    // the inherited types base classes
    PyObject* bases                 = nullptr,
    getbufferproc getbuffer         = nullptr,
    releasebufferproc releasebuffer = nullptr);

int YMQ_createIntEnum(
    PyObject* pyModule,
    OwnedPyObject<>* storage,
    std::string enumName,
    std::initializer_list<std::pair<const char*, int>> entries);

static int YMQ_exec(PyObject* pyModule);

static PyModuleDef_Slot YMQ_slots[] = {
    {Py_mod_exec, (void*)YMQ_exec},
    {0, nullptr},
};

static PyModuleDef YMQ_module = {
    .m_base     = PyModuleDef_HEAD_INIT,
    .m_name     = "_ymq",
    .m_doc      = PyDoc_STR("YMQ Python bindings"),
    .m_size     = sizeof(YMQState),
    .m_methods  = nullptr,
    .m_slots    = YMQ_slots,
    .m_traverse = nullptr,
    .m_clear    = nullptr,
    .m_free     = (freefunc)YMQ_free,
};

// this is a polyfill for PyErr_GetRaisedException() added in Python 3.12+
OwnedPyObject<> YMQ_GetRaisedException();

OwnedPyObject<> completeCallback(const OwnedPyObject<>& callback, const OwnedPyObject<>& result);

OwnedPyObject<> completeCallbackWithRaisedException(const OwnedPyObject<>& callback);

OwnedPyObject<> completeCallbackWithCoreError(
    YMQState* state, const OwnedPyObject<>& callback, const scaler::ymq::Error& error);

}  // namespace pymod
}  // namespace ymq
}  // namespace scaler

PyMODINIT_FUNC PyInit__ymq(void);

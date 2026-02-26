#pragma once

// Python
#include "scaler/utility/pymod/compatibility.h"

namespace scaler {
namespace uv_ymq {
namespace pymod {

using scaler::utility::pymod::OwnedPyObject;

struct UVYMQState {
    OwnedPyObject<> enumModule;  // Reference to the enum module

    OwnedPyObject<> PyIOContextType;  // Reference to the IOContext type

    OwnedPyObject<> PyBinderSocketType;  // Reference to the BinderSocket type

    OwnedPyObject<> PyAddressTypeEnumType;  // Reference to the Address.Type enum
    OwnedPyObject<> PyAddressType;          // Reference to the Address type
    OwnedPyObject<> PyErrorCodeType;        // Reference to the ErrorCode enum
    OwnedPyObject<> PyBytesType;            // Reference to Bytes type
    OwnedPyObject<> PyMessageType;          // Reference to Message type
    OwnedPyObject<> PyExceptionType;        // Reference to UVYMQException type

    std::unordered_map<int, OwnedPyObject<>> PyExceptionSubtypes;  // Map of error code to exception subclass
};

UVYMQState* UVYMQStateFromSelf(PyObject* self);

void UVYMQ_free(void* stateVoid);

// internal convenience function to create a type and add it to the module
static int UVYMQ_createType(
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

int UVYMQ_createIntEnum(
    PyObject* pyModule,
    OwnedPyObject<>* storage,
    std::string enumName,
    std::vector<std::pair<std::string, int>> entries);

static int UVYMQ_exec(PyObject* pyModule);

static PyModuleDef_Slot UVYMQ_slots[] = {
    {Py_mod_exec, (void*)UVYMQ_exec},
    {0, nullptr},
};

static PyModuleDef UVYMQ_module = {
    .m_base  = PyModuleDef_HEAD_INIT,
    .m_name  = "_uv_ymq",
    .m_doc   = PyDoc_STR("UV YMQ Python bindings"),
    .m_size  = sizeof(UVYMQState),
    .m_slots = UVYMQ_slots,
    .m_free  = (freefunc)UVYMQ_free,
};

}  // namespace pymod
}  // namespace uv_ymq
}  // namespace scaler

PyMODINIT_FUNC PyInit__uv_ymq(void);

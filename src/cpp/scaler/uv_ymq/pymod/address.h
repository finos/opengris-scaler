#pragma once

// Python
#include "scaler/utility/pymod/compatibility.h"

// C++
#include <memory>

// First-party
#include "scaler/uv_ymq/address.h"
#include "scaler/uv_ymq/pymod/uv_ymq.h"

namespace scaler {
namespace uv_ymq {
namespace pymod {

using scaler::utility::pymod::OwnedPyObject;

struct PyAddress {
    PyObject_HEAD;
    scaler::uv_ymq::Address address;
};

extern "C" {

int PyAddressType_createEnum(PyObject* pyModule, UVYMQState* state)
{
    std::vector<std::pair<std::string, int>> addressTypes = {
        {"IPC", (int)scaler::uv_ymq::Address::Type::IPC},
        {"TCP", (int)scaler::uv_ymq::Address::Type::TCP},
    };

    return UVYMQ_createIntEnum(pyModule, &state->PyAddressTypeEnumType, "AddressType", addressTypes);
}

static int PyAddress_init(PyAddress* self, PyObject* args, PyObject* kwds)
{
    const char* addressStr = nullptr;
    Py_ssize_t addressLen  = 0;
    const char* kwlist[]   = {"address", nullptr};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "s#", (char**)kwlist, &addressStr, &addressLen))
        return -1;

    auto result = scaler::uv_ymq::Address::fromString(std::string(addressStr, addressLen));
    if (!result.has_value()) {
        PyErr_SetString(PyExc_ValueError, result.error().what());
        return -1;
    }

    try {
        new (&self->address) scaler::uv_ymq::Address(std::move(result.value()));
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create Address");
        return -1;
    }

    return 0;
}

static void PyAddress_dealloc(PyAddress* self)
{
    try {
        self->address.~Address();
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to deallocate Address");
        PyErr_WriteUnraisable((PyObject*)self);
    }

    auto* tp = Py_TYPE(self);
    tp->tp_free(self);
    Py_DECREF(tp);
}

static PyObject* PyAddress_repr(PyAddress* self)
{
    auto result = self->address.toString();
    if (!result.has_value()) {
        PyErr_SetString(PyExc_RuntimeError, result.error().what());
        return nullptr;
    }

    return PyUnicode_FromStringAndSize(result.value().c_str(), result.value().size());
}

static PyObject* PyAddress_type_getter(PyAddress* self, void* Py_UNUSED(closure))
{
    auto state = UVYMQStateFromSelf((PyObject*)self);
    if (!state)
        return nullptr;

    const scaler::uv_ymq::Address::Type addressType = self->address.type();
    OwnedPyObject addressTypeIntObj                 = PyLong_FromLong((long)addressType);

    if (!addressTypeIntObj)
        return nullptr;

    return PyObject_CallOneArg(*state->PyAddressTypeEnumType, *addressTypeIntObj);
}

}  // extern "C"

static PyGetSetDef PyAddress_properties[] = {
    {"type", (getter)PyAddress_type_getter, nullptr, nullptr, nullptr},
    {nullptr, nullptr, nullptr, nullptr, nullptr},
};

static PyType_Slot PyAddress_slots[] = {
    {Py_tp_init, (void*)PyAddress_init},
    {Py_tp_dealloc, (void*)PyAddress_dealloc},
    {Py_tp_repr, (void*)PyAddress_repr},
    {Py_tp_getset, (void*)PyAddress_properties},
    {0, nullptr},
};

static PyType_Spec PyAddress_spec = {
    .name      = "_uv_ymq.Address",
    .basicsize = sizeof(PyAddress),
    .itemsize  = 0,
    .flags     = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_IMMUTABLETYPE,
    .slots     = PyAddress_slots,
};

}  // namespace pymod
}  // namespace uv_ymq
}  // namespace scaler

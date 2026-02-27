#pragma once

// Python
#include "scaler/utility/pymod/compatibility.h"

// First-party
#include "scaler/error/error.h"
#include "scaler/uv_ymq/pymod/uv_ymq.h"

namespace scaler {
namespace uv_ymq {
namespace pymod {

using scaler::utility::pymod::OwnedPyObject;

// the order of the members in the exception args tuple
const Py_ssize_t UVYMQException_errorCodeIndex = 0;
const Py_ssize_t UVYMQException_messageIndex   = 1;

struct UVYMQException {
    PyException_HEAD;
};

static int UVYMQException_init(UVYMQException* self, PyObject* args, PyObject* kwds)
{
    auto state = UVYMQStateFromSelf((PyObject*)self);
    if (!state)
        return -1;

    // no need to incref these because we don't store them
    // Furthermore, this fn does not create a strong reference to the args
    PyObject* errorCode    = nullptr;
    PyObject* errorMessage = nullptr;
    if (!PyArg_ParseTuple(args, "OO", &errorCode, &errorMessage))
        return -1;

    if (!PyObject_IsInstance(errorCode, *state->PyErrorCodeType)) {
        PyErr_SetString(PyExc_TypeError, "expected code to be of type ErrorCode");
        return -1;
    }

    if (!PyUnicode_Check(errorMessage)) {
        PyErr_SetString(PyExc_TypeError, "expected message to be a string");
        return -1;
    }

    // delegate to the base class init
    return ((PyTypeObject*)PyExc_Exception)->tp_init((PyObject*)self, args, kwds);
}

static void UVYMQException_dealloc(UVYMQException* self)
{
    // we still need to release the reference to the heap type
    auto* tp = Py_TYPE(self);
    ((PyTypeObject*)PyExc_Exception)->tp_dealloc((PyObject*)self);
    Py_DECREF(tp);
}

static PyObject* UVYMQException_code_getter(UVYMQException* self, void* Py_UNUSED(closure))
{
    return PySequence_GetItem(self->args, UVYMQException_errorCodeIndex);
}

static PyObject* UVYMQException_message_getter(UVYMQException* self, void* Py_UNUSED(closure))
{
    return PySequence_GetItem(self->args, UVYMQException_messageIndex);
}

static PyGetSetDef UVYMQException_getset[] = {
    {"code", (getter)UVYMQException_code_getter, nullptr, nullptr, nullptr},
    {"message", (getter)UVYMQException_message_getter, nullptr, nullptr, nullptr},
    {nullptr}  // Sentinel
};

static PyType_Slot UVYMQException_slots[] = {
    {Py_tp_init, (void*)UVYMQException_init},
    {Py_tp_dealloc, (void*)UVYMQException_dealloc},
    {Py_tp_getset, (void*)UVYMQException_getset},
    {0, 0},
};

static PyType_Spec UVYMQException_spec = {
    "_uv_ymq.UVYMQException",
    sizeof(UVYMQException),
    0,
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    UVYMQException_slots};

inline OwnedPyObject<> UVYMQException_argtupleFromCoreError(UVYMQState* state, const scaler::ymq::Error* error)
{
    OwnedPyObject code = PyLong_FromLong(static_cast<long>(error->_errorCode));

    if (!code)
        return nullptr;

    OwnedPyObject pyCode = PyObject_CallFunction(*state->PyErrorCodeType, "O", *code);

    if (!pyCode)
        return nullptr;

    OwnedPyObject message = PyUnicode_FromString(error->what());

    if (!message)
        return nullptr;

    return PyTuple_Pack(2, *pyCode, *message);
}

inline PyObject* UVYMQException_getTypeForError(UVYMQState* state, const scaler::ymq::Error* error)
{
    auto it = state->PyExceptionSubtypes.find(static_cast<int>(error->_errorCode));
    if (it != state->PyExceptionSubtypes.end()) {
        return *it->second;
    }
    return *state->PyExceptionType;
}

inline void UVYMQException_setFromCoreError(UVYMQState* state, const scaler::ymq::Error* error)
{
    auto tuple = UVYMQException_argtupleFromCoreError(state, error);
    if (!tuple)
        return;

    PyErr_SetObject(UVYMQException_getTypeForError(state, error), *tuple);
}

inline OwnedPyObject<> UVYMQException_createFromCoreError(UVYMQState* state, const scaler::ymq::Error* error)
{
    auto tuple = UVYMQException_argtupleFromCoreError(state, error);
    if (!tuple)
        return nullptr;

    return PyObject_CallObject(UVYMQException_getTypeForError(state, error), *tuple);
}

}  // namespace pymod
}  // namespace uv_ymq
}  // namespace scaler

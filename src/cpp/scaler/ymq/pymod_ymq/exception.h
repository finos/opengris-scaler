#pragma once

// Python
#include "scaler/ymq/pymod_ymq/compatibility.h"

// First-party
#include "scaler/error/error.h"
#include "scaler/ymq/pymod_ymq/ymq.h"

using namespace scaler::ymq;

// the order of the members in the exception args tuple
const Py_ssize_t YMQException_errorCodeIndex = 0;
const Py_ssize_t YMQException_messageIndex   = 1;

struct YMQException {
    PyException_HEAD;
};

extern "C" {

static int YMQException_init(YMQException* self, PyObject* args, PyObject* kwds)
{
    auto state = YMQStateFromSelf((PyObject*)self);
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
    return self->ob_base.ob_type->tp_base->tp_init((PyObject*)self, args, kwds);
}

static void YMQException_dealloc(YMQException* self)
{
    self->ob_base.ob_type->tp_base->tp_dealloc((PyObject*)self);

    // we still need to release the reference to the heap type
    auto* tp = Py_TYPE(self);
    Py_DECREF(tp);
}

static PyObject* YMQException_code_getter(YMQException* self, void* Py_UNUSED(closure))
{
    return PySequence_GetItem(self->args, YMQException_errorCodeIndex);
}

static PyObject* YMQException_message_getter(YMQException* self, void* Py_UNUSED(closure))
{
    return PySequence_GetItem(self->args, YMQException_messageIndex);
}
}

static PyGetSetDef YMQException_getset[] = {
    {"code", (getter)YMQException_code_getter, nullptr, PyDoc_STR("error code"), nullptr},
    {"message", (getter)YMQException_message_getter, nullptr, PyDoc_STR("error message"), nullptr},
    {nullptr}  // Sentinel
};

static PyType_Slot YMQException_slots[] = {
    {Py_tp_init, (void*)YMQException_init},
    {Py_tp_dealloc, (void*)YMQException_dealloc},
    {Py_tp_getset, (void*)YMQException_getset},
    {0, 0},
};

static PyType_Spec YMQException_spec = {
    "_ymq.YMQException", sizeof(YMQException), 0, Py_TPFLAGS_DEFAULT, YMQException_slots};

inline OwnedPyObject<> YMQException_argtupleFromCoreError(YMQState* state, const Error* error)
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

inline void YMQException_setFromCoreError(YMQState* state, const Error* error)
{
    auto tuple = YMQException_argtupleFromCoreError(state, error);
    if (!tuple)
        return;

    PyErr_SetObject(*state->PyExceptionType, *tuple);
}

inline OwnedPyObject<> YMQException_createFromCoreError(YMQState* state, const Error* error)
{
    auto tuple = YMQException_argtupleFromCoreError(state, error);
    if (!tuple)
        return nullptr;

    return PyObject_CallObject(*state->PyExceptionType, *tuple);
}

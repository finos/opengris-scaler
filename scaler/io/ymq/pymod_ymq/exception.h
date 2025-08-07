#pragma once

// Python
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>

// C++
#include <functional>

// First-party
#include "scaler/io/ymq/pymod_ymq/ymq.h"
#include "ymq.h"

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
    PyObject* code    = nullptr;
    PyObject* message = nullptr;
    if (!PyArg_ParseTuple(args, "OO", &code, &message))
        return -1;

    if (!PyObject_IsInstance(code, state->PyErrorCodeType)) {
        PyErr_SetString(PyExc_TypeError, "expected code to be of type ErrorCode");
        return -1;
    }

    if (!PyUnicode_Check(message)) {
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
    "ymq.YMQException", sizeof(YMQException), 0, Py_TPFLAGS_DEFAULT, YMQException_slots};

PyObject* YMQException_argtupleFromCoreError(YMQState* state, const Error* error)
{
    PyObject* code = PyLong_FromLong(static_cast<long>(error->_errorCode));

    if (!code)
        return nullptr;

    PyObject* pyCode = PyObject_CallFunction((PyObject*)state->PyErrorCodeType, "O", code);
    Py_DECREF(code);

    if (!pyCode)
        return nullptr;

    PyObject* message = PyUnicode_FromString(error->what());

    if (!message) {
        Py_DECREF(pyCode);
        return nullptr;
    }

    PyObject* tuple = PyTuple_Pack(2, pyCode, message);
    Py_DECREF(pyCode);
    Py_DECREF(message);

    return tuple;
}

void YMQException_setFromCoreError(YMQState* state, const Error* error)
{
    auto tuple = YMQException_argtupleFromCoreError(state, error);
    if (!tuple)
        return;

    PyErr_SetObject(state->PyExceptionType, tuple);
    Py_DECREF(tuple);
}

PyObject* YMQException_createFromCoreError(YMQState* state, const Error* error)
{
    auto tuple = YMQException_argtupleFromCoreError(state, error);
    if (!tuple)
        return nullptr;

    PyObject* exc = PyObject_CallObject(state->PyExceptionType, tuple);
    Py_DECREF(tuple);

    return exc;
}

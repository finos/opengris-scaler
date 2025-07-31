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

static int YMQException_init(YMQException* self, PyObject* args, PyObject* kwds) {
    // check the args
    PyObject* code    = nullptr;
    PyObject* message = nullptr;
    if (!PyArg_ParseTuple(args, "OO", &code, &message))
        return -1;

    // replace with PyType_GetModuleByDef(Py_TYPE(self), &ymq_module) in a newer Python version
    // https://docs.python.org/3/c-api/type.html#c.PyType_GetModuleByDef
    PyObject* pyModule = PyType_GetModule(Py_TYPE(self));
    if (!pyModule) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to get module for Message type");
        return -1;
    }

    auto state = (YMQState*)PyModule_GetState(pyModule);
    if (!state) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to get module state");
        return -1;
    }

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

static void YMQException_dealloc(YMQException* self) {
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject* YMQException_code_getter(YMQException* self, void* Py_UNUSED(closure)) {
    return PySequence_GetItem(self->args, YMQException_errorCodeIndex);
}

static PyObject* YMQException_message_getter(YMQException* self, void* Py_UNUSED(closure)) {
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

PyObject* YMQException_argtupleFromCoreError(const Error* error) {
    PyObject* code = PyLong_FromLong(static_cast<long>(error->_errorCode));

    if (!code)
        return nullptr;

    PyObject* message = PyUnicode_FromString(error->what());

    if (!message) {
        Py_DECREF(code);
        return nullptr;
    }

    PyObject* tuple = PyTuple_Pack(2, code, message);

    if (!tuple) {
        Py_DECREF(code);
        Py_DECREF(message);
        return nullptr;
    }

    Py_DECREF(code);
    Py_DECREF(message);

    return tuple;
}

void YMQException_setFromCoreError(YMQState* state, const Error* error) {
    auto tuple = YMQException_argtupleFromCoreError(error);
    if (!tuple)
        return;

    PyErr_SetObject(state->PyExceptionType, tuple);
    Py_DECREF(tuple);
}

PyObject* YMQException_createFromCoreError(YMQState* state, const Error* error) {
    auto tuple = YMQException_argtupleFromCoreError(error);
    if (!tuple)
        return nullptr;

    PyObject* exc = PyObject_CallObject(state->PyExceptionType, tuple);
    Py_DECREF(tuple);

    return exc;
}

#pragma once

#include <Python.h>
#include <structmember.h>

#include "descrobject.h"
#include "pyerrors.h"
#include "scaler/io/ymq/pymod_ymq/ymq.h"
#include "tupleobject.h"

typedef struct {
    PyException_HEAD;
} YmqException;

extern "C" {

static int YmqException_init(YmqException* self, PyObject* args, PyObject* kwds) {
    // check the args
    PyObject* error             = nullptr;
    static const char* kwlist[] = {"error", nullptr};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "O", (char**)kwlist, &error))
        return -1;

    // replace with PyType_GetModuleByDef(Py_TYPE(self), &ymq_module) in a newer Python version
    // https://docs.python.org/3/c-api/type.html#c.PyType_GetModuleByDef
    PyObject* module = PyType_GetModule(Py_TYPE(self));
    if (!module) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to get module for Message type");
        return -1;
    }

    auto state = (YmqState*)PyModule_GetState(module);
    if (!state) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to get module state");
        return -1;
    }

    if (!PyObject_IsInstance(error, state->errorEnum)) {
        PyErr_SetString(PyExc_TypeError, "expected a value of type Error");
        return -1;
    }

    Py_XINCREF(error);

    // delegate to the base class init
    return self->ob_base.ob_type->tp_base->tp_init((PyObject*)self, args, kwds);
}

static void YmqException_dealloc(YmqException* self) {
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject* YmqException_error_getter(YmqException* self, void* /*closure*/) {
    return PyTuple_GetItem(self->args, 0);  // error is the first item in args
}
}

static PyGetSetDef YmqException_getset[] = {
    {"error", (getter)YmqException_error_getter, nullptr, PyDoc_STR("error code"), nullptr}, {nullptr}  // Sentinel
};

static PyType_Slot YmqException_slots[] = {
    {Py_tp_init, (void*)YmqException_init},
    {Py_tp_dealloc, (void*)YmqException_dealloc},
    {Py_tp_getset, (void*)YmqException_getset},
    {0, 0}};

static PyType_Spec YmqException_spec = {
    "ymq.YmqException", sizeof(YmqException), 0, Py_TPFLAGS_DEFAULT, YmqException_slots};

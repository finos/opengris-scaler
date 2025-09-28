// Python
#pragma once
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>

#if PY_MAJOR_VERSION == 3 && PY_MINOR_VERSION == 8
static inline PyObject* Py_NewRef(PyObject* obj)
{
    Py_INCREF(obj);
    return obj;
}

static inline PyObject* Py_XNewRef(PyObject* obj)
{
    Py_XINCREF(obj);
    return obj;
}

// This is a very dirty hack, we basically place the raw pointer to this dict when init,
// see scaler/io/ymq/pymod_ymq/ymq.h for more detail
PyObject* PyType_GetModule(PyTypeObject* type)
{
    return PyObject_GetAttrString((PyObject*)(type), "__module_object__");
}

PyObject* PyType_GetModuleState(PyTypeObject* type)
{
    PyObject* module = PyObject_GetAttrString((PyObject*)(type), "__module_object__");
    if (!module)
        return NULL;
    void* state = PyModule_GetState(module);
    Py_DECREF(module);
    return (PyObject*)state;
}

static inline PyObject* PyObject_CallNoArgs(PyObject* callable)
{
    return PyObject_Call(callable, PyTuple_New(0), NULL);
}

static inline PyObject* PyObject_CallOneArg(PyObject* callable, PyObject* arg)
{
    PyObject* args = PyTuple_Pack(1, arg);
    if (!args)
        return NULL;
    PyObject* result = PyObject_Call(callable, args, NULL);
    Py_DECREF(args);
    return result;
}

static inline int PyModule_AddObjectRef(PyObject* mod, const char* name, PyObject* value)
{
    Py_INCREF(value);  // Since PyModule_AddObject steals a ref, we balance it
    return PyModule_AddObject(mod, name, value);
}

static inline PyObject* PyType_FromModuleAndSpec(PyObject* pymodule, PyType_Spec* spec, PyObject* bases)
{
    (void)pymodule;
    if (!bases) {
        bases = PyTuple_Pack(1, (PyObject*)&PyBaseObject_Type);
        if (!bases)
            return NULL;
        PyObject* res = PyType_FromSpecWithBases(spec, bases);
        Py_DECREF(bases);  // avoid leak
        return res;
    }
    return PyType_FromSpecWithBases(spec, bases);
}

#define Py_TPFLAGS_IMMUTABLETYPE          (0)
#define Py_TPFLAGS_DISALLOW_INSTANTIATION (0)
#endif

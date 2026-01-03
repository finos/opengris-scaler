#include "scaler/utility/one_to_many_dict.h"

// NOTE: To comply with this project's coding guideline, we should try to use
// #include <Python.h> here. However, because we handle this header file
// when the operating system is Windows and in DEBUG mode differently, it would
// be not possible to break another another rule which specifies system detail
// should be kept in implementation files. Maybe we should have a better way
// to run test pipeline for Windows. - gxu
#include "scaler/utility/pymod/compatibility.h"

extern "C" {
struct PyOneToManyDict {
    PyObject_HEAD;
    scaler::utility::OneToManyDict<OwnedPyObject<PyObject>, OwnedPyObject<PyObject>> dict;
};

static PyObject* PyOneToManyDictNew(PyTypeObject* type, PyObject* args, PyObject* kwds)
{
    PyOneToManyDict* self {};
    self = (PyOneToManyDict*)type->tp_alloc(type, 0);
    return (PyObject*)self;
}

static int PyOneToManyDictInit(PyOneToManyDict* self, PyObject* args, PyObject* kwds)
{
    new (&((PyOneToManyDict*)self)->dict)
        scaler::utility::OneToManyDict<OwnedPyObject<PyObject>, OwnedPyObject<PyObject>>();
    return 0;
}

static void PyOneToManyDictDealloc(PyObject* self)
{
    ((PyOneToManyDict*)self)->dict.~OneToManyDict<OwnedPyObject<PyObject>, OwnedPyObject<PyObject>>();
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject* PyOneToManyDictAdd(PyOneToManyDict* self, PyObject* args)
{
    PyObject* key {};
    PyObject* value {};

    if (!PyArg_ParseTuple(args, "OO", &key, &value)) {
        return nullptr;
    }

    if (self->dict.add(OwnedPyObject<>::fromBorrowed(key), OwnedPyObject<>::fromBorrowed(value))) {
        Py_RETURN_NONE;
    } else {
        PyErr_SetString(PyExc_ValueError, "value has to be unique in OneToManyDict");
        return nullptr;
    }
}

static PyObject* PyOneToManyDictHasKey(PyOneToManyDict* self, PyObject* args)
{
    PyObject* key {};
    if (!PyArg_ParseTuple(args, "O", &key)) {
        return nullptr;
    }

    if (self->dict.hasKey(OwnedPyObject<>::fromBorrowed(key))) {
        Py_RETURN_TRUE;
    } else {
        Py_RETURN_FALSE;
    }
}

static PyObject* PyOneToManyDictHasValue(PyOneToManyDict* self, PyObject* args)
{
    PyObject* value {};
    if (!PyArg_ParseTuple(args, "O", &value)) {
        return nullptr;
    }

    if (self->dict.hasValue(OwnedPyObject<>::fromBorrowed(value))) {
        Py_RETURN_TRUE;
    } else {
        Py_RETURN_FALSE;
    }
}

static PyObject* PyOneToManyDictGetKey(PyOneToManyDict* self, PyObject* args)
{
    PyObject* value {};
    if (!PyArg_ParseTuple(args, "O", &value)) {
        return nullptr;
    }

    const OwnedPyObject<>* key = self->dict.getKey(OwnedPyObject<>::fromBorrowed(value));
    if (!key) {
        PyErr_SetString(PyExc_ValueError, "cannot find value in OneToManyDict");
        return nullptr;
    }

    return OwnedPyObject(*key).take();
}

static PyObject* PyOneToManyDictGetValues(PyOneToManyDict* self, PyObject* args)
{
    PyObject* key {};
    if (!PyArg_ParseTuple(args, "O", &key)) {
        return nullptr;
    }

    const auto values = self->dict.getValues(OwnedPyObject<>::fromBorrowed(key));
    if (!values) {
        PyErr_SetString(PyExc_ValueError, "cannot find key in OneToManyDict");
        return nullptr;
    }

    OwnedPyObject<> valueSet = PySet_New(nullptr);
    if (!valueSet) {
        return nullptr;
    }

    for (const auto& value: *values) {
        if (PySet_Add(*valueSet, *value) == -1) {
            return nullptr;
        }
    }

    return valueSet.take();
}

static PyObject* PyOneToManyDictRemoveKey(PyOneToManyDict* self, PyObject* args)
{
    PyObject* key {};
    if (!PyArg_ParseTuple(args, "O", &key)) {
        return nullptr;
    }

    auto result = self->dict.removeKey(OwnedPyObject<>::fromBorrowed(key));
    if (!result.second) {
        PyErr_SetString(PyExc_KeyError, "cannot find key in OneToManyDict");
        return nullptr;
    }

    OwnedPyObject<> valueSet = PySet_New(nullptr);
    if (!valueSet) {
        return nullptr;
    }

    for (const auto& value: result.first) {
        if (PySet_Add(*valueSet, *value) == -1) {
            return nullptr;
        }
    }

    return valueSet.take();
}

static PyObject* PyOneToManyDictRemoveValue(PyOneToManyDict* self, PyObject* args)
{
    PyObject* value {};
    if (!PyArg_ParseTuple(args, "O", &value)) {
        return nullptr;
    }

    auto result = self->dict.removeValue(OwnedPyObject<>::fromBorrowed(value));
    if (!result.second) {
        PyErr_SetString(PyExc_ValueError, "cannot find value in OneToManyDict");
        return nullptr;
    }

    return result.first.take();
}

static PyObject* PyOneToManyDictKeys(PyOneToManyDict* self, PyObject* args)
{
    OwnedPyObject<> keySet = PySet_New(nullptr);
    if (!keySet) {
        return nullptr;
    }

    for (const auto& entry: self->dict.keys()) {
        if (PySet_Add(*keySet, *entry.first) == -1) {
            return nullptr;
        }
    }

    return keySet.take();
}

// C++ function to return all values (which are keys in the C++ map)
static PyObject* PyOneToManyDictValues(PyOneToManyDict* self, PyObject* args)
{
    const auto& keyToValue = self->dict.keys();

    OwnedPyObject<> resultList = PyList_New(0);

    if (!resultList) {
        return nullptr;
    }

    for (const auto& [k, vs]: keyToValue) {
        OwnedPyObject<> pySet = PySet_New(nullptr);

        if (!pySet) {
            return nullptr;
        }

        for (const auto& v: vs) {
            if (PySet_Add(*pySet, *v) == -1) {
                return nullptr;
            }
        }
        if (PyList_Append(*resultList, *pySet) == -1) {
            return nullptr;
        }
    }

    return resultList.take();
}

static PyObject* PyOneToManyDictItems(PyOneToManyDict* self, PyObject* args)
{
    OwnedPyObject<> itemList = PyList_New(0);
    if (!itemList) {
        return nullptr;
    }

    for (const auto& [key, values]: self->dict.keys()) {
        OwnedPyObject<> valueSet = PySet_New(nullptr);
        if (!valueSet) {
            return nullptr;
        }

        for (const auto& value: values) {
            if (PySet_Add(*valueSet, *value) == -1) {
                return nullptr;
            }
        }

        OwnedPyObject<> itemTuple = PyTuple_Pack(2, *key, *valueSet);
        if (!itemTuple) {
            return nullptr;
        }

        if (PyList_Append(*itemList, *itemTuple) == -1) {
            return nullptr;
        }
    }

    return itemList.take();
}

// called when using the 'in' operator (__contains__)
static PyObject* PyOneToManyDictContains(PyOneToManyDict* self, PyObject* args)
{
    PyObject* key {};
    if (!PyArg_ParseTuple(args, "O", &key)) {
        return nullptr;  // Invalid arguments
    }

    if (self->dict.hasKey(OwnedPyObject<>::fromBorrowed(key))) {
        Py_RETURN_TRUE;
    } else {
        Py_RETURN_FALSE;
    }
}

static PyObject* PyOneToManyDictIter(PyObject* self)
{
    PyOneToManyDict* obj = (PyOneToManyDict*)self;
    obj->dict._iter      = obj->dict._keyToValues.begin();

    Py_INCREF(self);
    return self;
}

static PyObject* PyOneToManyDictIterNext(PyObject* self)
{
    PyOneToManyDict* obj = (PyOneToManyDict*)self;
    if (obj->dict._iter == obj->dict._keyToValues.end()) {
        return nullptr;  // Stop iteration (signals the end)
    }

    auto key = (obj->dict._iter)->first;
    ++obj->dict._iter;
    return key.take();
}

// Define the methods for the OneToManyDict Python class
static PyMethodDef PyOneToManyDictMethods[] = {
    {"__contains__", (PyCFunction)PyOneToManyDictContains, METH_VARARGS, "__contains__ method"},
    {"keys", (PyCFunction)PyOneToManyDictKeys, METH_VARARGS, "Get Keys from the dictionary"},
    {"values", (PyCFunction)PyOneToManyDictValues, METH_VARARGS, "Get Values from the dictionary"},
    {"items", (PyCFunction)PyOneToManyDictItems, METH_VARARGS, "Get Items from the dictionary"},
    {"add", (PyCFunction)PyOneToManyDictAdd, METH_VARARGS, "Add a key-value pair to the dictionary"},
    {"has_key", (PyCFunction)PyOneToManyDictHasKey, METH_VARARGS, "Check if a key exists in the dictionary"},
    {"has_value", (PyCFunction)PyOneToManyDictHasValue, METH_VARARGS, "Check if a value exists in the dictionary"},
    {"get_key", (PyCFunction)PyOneToManyDictGetKey, METH_VARARGS, "Get a key from the dictionary"},
    {"get_values", (PyCFunction)PyOneToManyDictGetValues, METH_VARARGS, "Get values from the dictionary"},
    {"remove_key",
     (PyCFunction)PyOneToManyDictRemoveKey,
     METH_VARARGS,
     "Remove key from the dictionary and return associated values"},
    {"remove_value",
     (PyCFunction)PyOneToManyDictRemoveValue,
     METH_VARARGS,
     "Remove value from the dictionary and return the associated key"},
    {nullptr},
};

// Define the Python Object Type for OneToManyDict
static PyTypeObject PyOneToManyDictType = {
    .tp_name      = "one_to_many_dict.OneToManyDict",
    .tp_basicsize = sizeof(PyOneToManyDict),
    .tp_dealloc   = (destructor)PyOneToManyDictDealloc,
    .tp_flags     = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .tp_doc       = "OneToManyDict",
    .tp_iter      = (getiterfunc)PyOneToManyDictIter,
    .tp_iternext  = (iternextfunc)PyOneToManyDictIterNext,
    .tp_methods   = PyOneToManyDictMethods,
    .tp_init      = (initproc)PyOneToManyDictInit,
    .tp_new       = PyOneToManyDictNew,
};

static PyModuleDef one_to_many_dict_module = {
    .m_base  = PyModuleDef_HEAD_INIT,
    .m_name  = "one_to_many_dict",
    .m_doc   = PyDoc_STR("A module that wraps a C++ OneToManyDict class"),
    .m_size  = 0,
    .m_slots = nullptr,
    .m_free  = nullptr,
};

// Module initialization function
PyMODINIT_FUNC PyInit_one_to_many_dict(void)
{
    PyObject* m {};

    if (PyType_Ready(&PyOneToManyDictType) < 0) {
        return nullptr;
    }

    m = PyModule_Create(&one_to_many_dict_module);
    if (!m) {
        return nullptr;
    }

    Py_INCREF(&PyOneToManyDictType);
    PyModule_AddObject(m, "OneToManyDict", (PyObject*)&PyOneToManyDictType);

    return m;
}
}

#include "scaler/utility/one_to_many_dict.h"

// NOTE: To comply with this project's coding guideline, we should try to use
// #include <Python.h> here. However, because we handle this header file
// when the operating system is Windows and in DEBUG mode differently, it would
// be not possible to break another another rule which specifies system detail
// should be kept in implementation files. Maybe we should have a better way
// to run test pipeline for Windows. - gxu
#include "scaler/utility/pymod/compatibility.h"

namespace scaler {
namespace utility {
namespace one_to_many_dict {
namespace pymod {

using scaler::utility::pymod::OwnedPyObject;

extern "C" {
struct PyOneToManyDict {
    PyObject_HEAD;
    scaler::utility::OneToManyDict<OwnedPyObject<>, OwnedPyObject<>> dict;
};

struct PyOneToManyDictIterator {
    PyObject_HEAD;
    PyOneToManyDict* dict;  // keep container alive
    scaler::utility::OneToManyDict<OwnedPyObject<>, OwnedPyObject<>>::IterType iter;
    scaler::utility::OneToManyDict<OwnedPyObject<>, OwnedPyObject<>>::BucketCountType bucketCount;
};

static PyObject* PyOneToManyDictNew(PyTypeObject* type, PyObject* args, PyObject* kwds)
{
    return type->tp_alloc(type, 0);
}

static int PyOneToManyDictInit(PyOneToManyDict* self, PyObject* args, PyObject* kwds)
{
    new (&(self->dict)) scaler::utility::OneToManyDict<OwnedPyObject<>, OwnedPyObject<>>();
    return 0;
}

static void PyOneToManyDictDealloc(PyObject* self)
{
    ((PyOneToManyDict*)self)->dict.~OneToManyDict<OwnedPyObject<>, OwnedPyObject<>>();
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

static int PyOneToManyDictContains(PyObject* self, PyObject* key)
{
    return ((PyOneToManyDict*)self)->dict.hasKey(OwnedPyObject<>::fromBorrowed(key));
}

// Define the methods for the OneToManyDict Python class
static PyMethodDef PyOneToManyDictMethods[] = {
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

static PyObject* PyOneToManyDictIteratorIter(PyObject* self);
static PyObject* PyOneToManyDictIteratorIterNext(PyObject* self);
static PyObject* PyOneToManyDictIteratorIterSelf(PyObject* self);

static void PyOneToManyDictIteratorDealloc(PyObject* self) noexcept
{
    auto* it = (PyOneToManyDictIterator*)self;

    Py_XDECREF(it->dict);
    Py_TYPE(self)->tp_free(self);
}

static PyType_Slot PyOneToManyDictSlots[] = {
    {Py_tp_dealloc, (void*)PyOneToManyDictDealloc},
    {Py_tp_init, (void*)PyOneToManyDictInit},
    {Py_tp_new, (void*)PyOneToManyDictNew},
    {Py_tp_methods, PyOneToManyDictMethods},
    {Py_tp_iter, (void*)PyOneToManyDictIteratorIter},
    {Py_sq_contains, (void*)PyOneToManyDictContains},
    {0, nullptr},
};

static PyModuleDef one_to_many_dict_module = {
    .m_base  = PyModuleDef_HEAD_INIT,
    .m_name  = "one_to_many_dict",
    .m_doc   = PyDoc_STR("A module that wraps the C++ OneToManyDict class"),
    .m_size  = 0,
    .m_slots = nullptr,
    .m_free  = nullptr,
};

static PyType_Spec PyOneToManyDictSpec = {
    .name      = "one_to_many_dict.OneToManyDict",
    .basicsize = sizeof(PyOneToManyDict),
    .flags     = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .slots     = PyOneToManyDictSlots,
};

static PyType_Slot PyOneToManyDictIteratorSlots[] = {
    {Py_tp_dealloc, (void*)PyOneToManyDictIteratorDealloc},
    {Py_tp_iternext, (void*)PyOneToManyDictIteratorIterNext},
    {Py_tp_iter, (void*)PyOneToManyDictIteratorIterSelf},
    {0, nullptr},
};

static PyType_Spec PyOneToManyDictIteratorSpec = {
    .name      = "one_to_many_dict.OneToManyDictIterator",
    .basicsize = sizeof(PyOneToManyDictIterator),
    .itemsize  = 0,
    .flags     = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_DISALLOW_INSTANTIATION,
    .slots     = PyOneToManyDictIteratorSlots,
};

static PyObject* PyOneToManyDictIteratorIter(PyObject* self)
{
    static PyObject* iterType = nullptr;

    if (!iterType) {
        iterType = PyType_FromSpec(&PyOneToManyDictIteratorSpec);
        if (!iterType) {
            return nullptr;
        }
    }

    auto* it = (PyOneToManyDictIterator*)((PyTypeObject*)iterType)->tp_alloc((PyTypeObject*)iterType, 0);

    if (!it) {
        return nullptr;
    }

    Py_INCREF(self);
    it->dict = (PyOneToManyDict*)self;

    const auto safeIter = it->dict->dict.safeKeyToValueBegin();
    it->iter            = std::move(safeIter.first);
    it->bucketCount     = std::move(safeIter.second);

    return (PyObject*)it;
}

static PyObject* PyOneToManyDictIteratorIterNext(PyObject* self)
{
    PyOneToManyDictIterator* obj = (PyOneToManyDictIterator*)self;

    if (!obj->dict->dict.keyToValueIteratorValid(obj->bucketCount)) {
        PyErr_SetString(PyExc_RuntimeError, "Iterator invalidated during iteration");
        return nullptr;
    }

    if (obj->iter == obj->dict->dict._keyToValues.end()) {
        PyErr_SetNone(PyExc_StopIteration);
        return nullptr;
    }

    auto key = (obj->iter)->first;
    // Validity is guaranteed to be true as previously checked
    obj->iter = obj->dict->dict.safeKeyToValueNext(obj->iter, obj->bucketCount).first;
    return key.take();
}

static PyObject* PyOneToManyDictIteratorIterSelf(PyObject* self)
{
    return OwnedPyObject<>::fromBorrowed(self).take();
}
}
}  // namespace pymod
}  // namespace one_to_many_dict
}  // namespace utility

PyMODINIT_FUNC PyInit_one_to_many_dict(void)
{
    using scaler::utility::one_to_many_dict::pymod::one_to_many_dict_module;
    using scaler::utility::one_to_many_dict::pymod::PyOneToManyDictSpec;

    PyObject* m = PyModule_Create(&one_to_many_dict_module);
    if (!m) {
        return nullptr;
    }

    PyObject* type = PyType_FromSpec(&PyOneToManyDictSpec);
    if (!type) {
        Py_DECREF(m);
        return nullptr;
    }

    if (PyModule_AddObject(m, "OneToManyDict", type) < 0) {
        Py_DECREF(type);
        Py_DECREF(m);
        return nullptr;
    }

    return m;
}
}  // namespace scaler

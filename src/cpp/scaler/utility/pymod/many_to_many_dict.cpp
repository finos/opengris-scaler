
#include "scaler/utility/many_to_many_dict.h"

#include "scaler/utility/pymod/compatibility.h"

namespace scaler {
namespace utility {
namespace many_to_many_dict {
namespace pymod {

using scaler::utility::pymod::OwnedPyObject;

extern "C" {
struct PyManyToManyDict {
    PyObject_HEAD;
    scaler::utility::ManyToManyDict<OwnedPyObject<>, OwnedPyObject<>> dict;
};

static PyObject* PyManyToManyDictNew(PyTypeObject* type, PyObject* args, PyObject* kwds)
{
    return type->tp_alloc(type, 0);
}

static int PyManyToManyDictInit(PyManyToManyDict* self, PyObject* args, PyObject* kwds)
{
    new (&(self->dict)) scaler::utility::ManyToManyDict<OwnedPyObject<>, OwnedPyObject<>>();
    return 0;
}

static void PyManyToManyDictDealloc(PyObject* self)
{
    ((PyManyToManyDict*)self)->dict.~ManyToManyDict<OwnedPyObject<>, OwnedPyObject<>>();
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject* PyManyToManyDictAdd(PyManyToManyDict* self, PyObject* args)
{
    PyObject* leftKey {};
    PyObject* rightKey {};

    if (!PyArg_ParseTuple(args, "OO", &leftKey, &rightKey)) {
        return nullptr;
    }

    self->dict.add(OwnedPyObject<>::fromBorrowed(leftKey), OwnedPyObject<>::fromBorrowed(rightKey));
    Py_RETURN_NONE;
}

static PyObject* PyManyToManyDictHasLeftKey(PyManyToManyDict* self, PyObject* args)
{
    PyObject* key {};
    if (!PyArg_ParseTuple(args, "O", &key)) {
        return nullptr;
    }

    if (self->dict.hasLeftKey(OwnedPyObject<>::fromBorrowed(key))) {
        Py_RETURN_TRUE;
    } else {
        Py_RETURN_FALSE;
    }
}

static PyObject* PyManyToManyDictHasRightKey(PyManyToManyDict* self, PyObject* args)
{
    PyObject* key {};
    if (!PyArg_ParseTuple(args, "O", &key)) {
        return nullptr;
    }

    if (self->dict.hasRightKey(OwnedPyObject<>::fromBorrowed(key))) {
        Py_RETURN_TRUE;
    } else {
        Py_RETURN_FALSE;
    }
}

static PyObject* PyManyToManyDictHasKeyPair(PyManyToManyDict* self, PyObject* args)
{
    PyObject* leftKey {};
    PyObject* rightKey {};
    if (!PyArg_ParseTuple(args, "OO", &leftKey, &rightKey)) {
        return nullptr;
    }

    if (self->dict.hasKeyPair(OwnedPyObject<>::fromBorrowed(leftKey), OwnedPyObject<>::fromBorrowed(rightKey))) {
        Py_RETURN_TRUE;
    } else {
        Py_RETURN_FALSE;
    }
}

static PyObject* PyManyToManyDictLeftKeys(PyManyToManyDict* self, PyObject* args)
{
    const auto& leftKeys = self->dict.leftKeys();

    OwnedPyObject<> leftKeysSet = PySet_New(nullptr);
    if (!leftKeysSet) {
        return nullptr;
    }

    for (const auto& [leftKey, _]: leftKeys) {
        if (PySet_Add(*leftKeysSet, *leftKey) == -1) {
            return nullptr;
        }
    }

    return leftKeysSet.take();
}

static PyObject* PyManyToManyDictRightKeys(PyManyToManyDict* self, PyObject* args)
{
    const auto& rightKeys = self->dict.rightKeys();

    OwnedPyObject<> rightKeysSet = PySet_New(nullptr);
    if (!rightKeysSet) {
        return nullptr;
    }

    for (const auto& [rightKey, _]: rightKeys) {
        if (PySet_Add(*rightKeysSet, *rightKey) == -1) {
            return nullptr;
        }
    }

    return rightKeysSet.take();
}

static PyObject* PyManyToManyDictRemove(PyManyToManyDict* self, PyObject* args)
{
    PyObject* leftKey {};
    PyObject* rightKey {};
    if (!PyArg_ParseTuple(args, "OO", &leftKey, &rightKey)) {
        return nullptr;
    }

    auto result = self->dict.remove(OwnedPyObject<>::fromBorrowed(leftKey), OwnedPyObject<>::fromBorrowed(rightKey));
    if (!result) {
        PyErr_SetString(PyExc_KeyError, "cannot find key or value in dictionary");
        return nullptr;
    }
    Py_RETURN_NONE;
}

static PyObject* PyManyToManyDictLeftKeyItems(PyManyToManyDict* self, PyObject* args)
{
    OwnedPyObject<> itemList = PyList_New(0);
    if (!itemList) {
        return nullptr;
    }

    for (const auto& [key, values]: self->dict.leftKeyItems()) {
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

static PyObject* PyManyToManyDictRightKeyItems(PyManyToManyDict* self, PyObject* args)
{
    OwnedPyObject<> itemList = PyList_New(0);
    if (!itemList) {
        return nullptr;
    }

    for (const auto& [key, values]: self->dict.rightKeyItems()) {
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

static PyObject* PyManyToManyDictGetLeftItems(PyManyToManyDict* self, PyObject* args)
{
    PyObject* rightKey {};
    if (!PyArg_ParseTuple(args, "O", &rightKey)) {
        return nullptr;
    }

    const auto& [leftKeys, valid] = self->dict.getLeftItems(OwnedPyObject<>::fromBorrowed(rightKey));

    if (!valid) {
        PyErr_SetString(PyExc_ValueError, "cannot find leftKey in dictionary");
        return nullptr;
    }

    OwnedPyObject<> leftKeysSet = PySet_New(nullptr);
    if (!leftKeysSet) {
        return nullptr;
    }

    for (const auto& leftKey: *leftKeys) {
        if (PySet_Add(*leftKeysSet, *leftKey) == -1) {
            return nullptr;
        }
    }

    return leftKeysSet.take();
}

static PyObject* PyManyToManyDictGetRightItems(PyManyToManyDict* self, PyObject* args)
{
    PyObject* leftKey {};
    if (!PyArg_ParseTuple(args, "O", &leftKey)) {
        return nullptr;
    }

    const auto& [rightKeys, valid] = self->dict.getRightItems(OwnedPyObject<>::fromBorrowed(leftKey));

    if (!valid) {
        PyErr_SetString(PyExc_ValueError, "cannot find leftKey in dictionary");
        return nullptr;
    }

    OwnedPyObject<> rightKeysSet = PySet_New(nullptr);
    if (!rightKeysSet) {
        return nullptr;
    }

    for (const auto& rightKey: *rightKeys) {
        if (PySet_Add(*rightKeysSet, *rightKey) == -1) {
            return nullptr;
        }
    }

    return rightKeysSet.take();
}

static PyObject* PyManyToManyDictRemoveLeftKey(PyManyToManyDict* self, PyObject* args)
{
    PyObject* leftKey {};
    if (!PyArg_ParseTuple(args, "O", &leftKey)) {
        return nullptr;
    }

    auto [rightKeys, valid] = self->dict.removeLeftKey(OwnedPyObject<>::fromBorrowed(leftKey));

    if (!valid) {
        PyErr_SetString(PyExc_KeyError, "cannot find leftKey in dictionary");
        return nullptr;
    }

    OwnedPyObject<> rightKeysSet = PySet_New(nullptr);
    if (!rightKeysSet) {
        return nullptr;
    }

    for (const auto& rightKey: rightKeys) {
        if (PySet_Add(*rightKeysSet, *rightKey) == -1) {
            return nullptr;
        }
    }

    return rightKeysSet.take();
}

static PyObject* PyManyToManyDictRemoveRightKey(PyManyToManyDict* self, PyObject* args)
{
    PyObject* rightKey {};
    if (!PyArg_ParseTuple(args, "O", &rightKey)) {
        return nullptr;
    }

    auto [leftKeys, valid] = self->dict.removeRightKey(OwnedPyObject<>::fromBorrowed(rightKey));

    if (!valid) {
        PyErr_SetString(PyExc_KeyError, "cannot find rightKey in dictionary");
        return nullptr;
    }

    OwnedPyObject<> leftKeysSet = PySet_New(nullptr);
    if (!leftKeysSet) {
        return nullptr;
    }

    for (const auto& leftKey: leftKeys) {
        if (PySet_Add(*leftKeysSet, *leftKey) == -1) {
            return nullptr;
        }
    }

    return leftKeysSet.take();
}

static PyObject* PyManyToManyDictClassGetItem(PyObject* cls, PyObject* args)
{
    Py_INCREF(cls);
    return cls;
}

// Define the methods for the ManyToManyDict Python class
static PyMethodDef PyManyToManyDictMethods[] = {
    {"left_keys", (PyCFunction)PyManyToManyDictLeftKeys, METH_VARARGS, "Get left keys from the dictionary"},
    {"right_keys", (PyCFunction)PyManyToManyDictRightKeys, METH_VARARGS, "Get right keys from the dictionary"},
    {"add", (PyCFunction)PyManyToManyDictAdd, METH_VARARGS, "Add a pair of keys to the dictionary"},
    {"remove", (PyCFunction)PyManyToManyDictRemove, METH_VARARGS, "Remove a pair of keys from the dictionary"},
    {"has_left_key",
     (PyCFunction)PyManyToManyDictHasLeftKey,
     METH_VARARGS,
     "Check if a left key exists in the dictionary"},
    {"has_right_key",
     (PyCFunction)PyManyToManyDictHasRightKey,
     METH_VARARGS,
     "Check if a right key exists in the dictionary"},
    {"has_key_pair",
     (PyCFunction)PyManyToManyDictHasKeyPair,
     METH_VARARGS,
     "Check if a pair of keys exists in the dictionary"},
    {"left_key_items",
     (PyCFunction)PyManyToManyDictLeftKeyItems,
     METH_VARARGS,
     "Get left key items from the dictionary"},
    {"right_key_items",
     (PyCFunction)PyManyToManyDictRightKeyItems,
     METH_VARARGS,
     "Get right key items from the dictionary"},
    {"get_left_items", (PyCFunction)PyManyToManyDictGetLeftItems, METH_VARARGS, "Get left key items of a right key"},
    {"get_right_items", (PyCFunction)PyManyToManyDictGetRightItems, METH_VARARGS, "Get right key items of a left key"},
    {"remove_left_key",
     (PyCFunction)PyManyToManyDictRemoveLeftKey,
     METH_VARARGS,
     "Remove a left key from the dictionary and return all associated right keys"},
    {"remove_right_key",
     (PyCFunction)PyManyToManyDictRemoveRightKey,
     METH_VARARGS,
     "Remove a right key from the dictionary and return all associated left keys"},
    {"__class_getitem__", (PyCFunction)PyManyToManyDictClassGetItem, METH_CLASS | METH_VARARGS, "__class_getitem__"},
    {nullptr},
};

static PyType_Slot PyManyToManyDictSlots[] = {
    {Py_tp_dealloc, (void*)PyManyToManyDictDealloc},
    {Py_tp_init, (void*)PyManyToManyDictInit},
    {Py_tp_new, (void*)PyManyToManyDictNew},
    {Py_tp_methods, PyManyToManyDictMethods},
    {0, nullptr},
};

static PyModuleDef many_to_many_dict_module = {
    .m_base  = PyModuleDef_HEAD_INIT,
    .m_name  = "many_to_many_dict",
    .m_doc   = PyDoc_STR("A module that wraps the C++ ManyToManyDict class"),
    .m_size  = 0,
    .m_slots = nullptr,
    .m_free  = nullptr,
};

static PyType_Spec PyManyToManyDictSpec = {
    .name      = "many_to_many_dict.ManyToManyDict",
    .basicsize = sizeof(PyManyToManyDict),
    .flags     = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .slots     = PyManyToManyDictSlots,
};
}
}  // namespace pymod
}  // namespace many_to_many_dict
}  // namespace utility

PyMODINIT_FUNC PyInit_many_to_many_dict(void)
{
    using scaler::utility::many_to_many_dict::pymod::many_to_many_dict_module;
    using scaler::utility::many_to_many_dict::pymod::PyManyToManyDictSpec;
    PyObject* m = PyModule_Create(&many_to_many_dict_module);
    if (!m) {
        return nullptr;
    }

    PyObject* type = PyType_FromSpec(&PyManyToManyDictSpec);
    if (!type) {
        Py_DECREF(m);
        return nullptr;
    }

    if (PyModule_AddObject(m, "ManyToManyDict", type) < 0) {
        Py_DECREF(type);
        Py_DECREF(m);
        return nullptr;
    }

    return m;
}
}  // namespace scaler

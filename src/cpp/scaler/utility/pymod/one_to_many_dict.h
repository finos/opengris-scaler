#include <Python.h>

#include "scaler/utility/one_to_many_dict.h"

// Structure to wrap the OneToManyDict object in Python
typedef struct {
    PyObject_HEAD;
    OneToManyDict<PyObject*, PyObject*>* dict;  // Pointer to C++ OneToManyDict
} OneToManyDictObject;

extern "C" {

// Forward declarations of methods
static void OneToManyDict_dealloc(OneToManyDictObject* self);
static PyObject* OneToManyDict_new(PyTypeObject* type, PyObject* args, PyObject* kwds);
static int OneToManyDict_init(OneToManyDictObject* self, PyObject* args, PyObject* kwds);

// Add method for OneToManyDict
static PyObject* OneToManyDict_add(OneToManyDictObject* self, PyObject* args);
static PyObject* OneToManyDict_getValues(OneToManyDictObject* self, PyObject* args);
static PyObject* OneToManyDict_removeKey(OneToManyDictObject* self, PyObject* args);
static PyObject* OneToManyDict_removeValue(OneToManyDictObject* self, PyObject* args);

// Define the methods for the OneToManyDict Python class
static PyMethodDef OneToManyDict_methods[] = {
    {"add", (PyCFunction)OneToManyDict_add, METH_VARARGS, "Add a key-value pair to the dictionary"},
    {"getValues", (PyCFunction)OneToManyDict_getValues, METH_VARARGS, "Get values for a key"},
    {"removeKey", (PyCFunction)OneToManyDict_removeKey, METH_VARARGS, "Remove a key from the dictionary"},
    {"removeValue", (PyCFunction)OneToManyDict_removeValue, METH_VARARGS, "Remove a value from the dictionary"},
    {NULL}  // Sentinel
};

static PyTypeObject PyObjectStorageServerType = {};

// Define the Python Object Type for OneToManyDict
static PyTypeObject OneToManyDictType = {
    .tp_name      = "one_to_many_dict.OneToManyDict",
    .tp_basicsize = sizeof(OneToManyDictObject),
    .tp_dealloc   = (destructor)OneToManyDict_dealloc,
    .tp_flags     = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .tp_doc       = "OneToManyDict",
    .tp_methods   = OneToManyDict_methods,
    .tp_init      = (initproc)OneToManyDict_init,
    .tp_new       = OneToManyDict_new,
};

// New function for creating instances of OneToManyDict
static PyObject* OneToManyDict_new(PyTypeObject* type, PyObject* args, PyObject* kwds)
{
    OneToManyDictObject* self;
    self = (OneToManyDictObject*)type->tp_alloc(type, 0);
    if (self != NULL) {
        self->dict = new OneToManyDict<PyObject*, PyObject*>();
    }
    return (PyObject*)self;
}

// Init function for the OneToManyDict Python object
static int OneToManyDict_init(OneToManyDictObject* self, PyObject* args, PyObject* kwds)
{
    // You can initialize from args here if needed
    return 0;
}

// Deallocation function to clean up memory
static void OneToManyDict_dealloc(OneToManyDictObject* self)
{
    delete self->dict;                        // Clean up the C++ object
    Py_TYPE(self)->tp_free((PyObject*)self);  // Free the Python object
}

// Add method for OneToManyDict
static PyObject* OneToManyDict_add(OneToManyDictObject* self, PyObject* args)
{
    PyObject* key;
    PyObject* value;

    if (!PyArg_ParseTuple(args, "OO", &key, &value)) {
        return NULL;  // Invalid arguments
    }

    if (self->dict->add(key, value)) {
        Py_RETURN_TRUE;
    } else {
        Py_RETURN_FALSE;
    }
}

// Get values for a key
static PyObject* OneToManyDict_getValues(OneToManyDictObject* self, PyObject* args)
{
    PyObject* key;
    if (!PyArg_ParseTuple(args, "O", &key)) {
        return NULL;  // Invalid arguments
    }

    const std::unordered_set<PyObject*>* values = self->dict->getValues(key);
    if (values == nullptr) {
        Py_RETURN_NONE;
    }

    PyObject* result = PyList_New(values->size());
    size_t i         = 0;
    for (const auto& value: *values) {
        PyList_SET_ITEM(result, i++, value);  // Add each value to the result list
    }

    return result;
}

// Remove a key from the dictionary
static PyObject* OneToManyDict_removeKey(OneToManyDictObject* self, PyObject* args)
{
    PyObject* key;
    if (!PyArg_ParseTuple(args, "O", &key)) {
        return NULL;
    }

    auto res = self->dict->removeKey(key);
    if (res.second) {
        PyObject* result_set = PyList_New(res.first.size());
        size_t i             = 0;
        for (auto& val: res.first) {
            PyList_SET_ITEM(result_set, i++, val);
        }
        return result_set;
    }
    Py_RETURN_NONE;
}

// Remove a value from the dictionary
static PyObject* OneToManyDict_removeValue(OneToManyDictObject* self, PyObject* args)
{
    PyObject* value;
    if (!PyArg_ParseTuple(args, "O", &value)) {
        return NULL;
    }

    auto res = self->dict->removeValue(value);
    if (res.second) {
        return res.first;
    }
    Py_RETURN_NONE;
}
}

static PyMethodDef module_methods[] = {{NULL}};

// Module definition
static struct PyModuleDef one_to_many_dict_module = {
    PyModuleDef_HEAD_INIT,
    "one_to_many_dict",
    "A module that wraps a C++ OneToManyDict class",
    -1,             // Size of per-interpreter state of the module
    module_methods  // Methods of the module
};

// Module initialization function
PyMODINIT_FUNC PyInit_one_to_many_dict(void)
{
    PyObject* m;

    if (PyType_Ready(&OneToManyDictType) < 0)
        return NULL;

    m = PyModule_Create(&one_to_many_dict_module);
    if (m == NULL)
        return NULL;

    Py_INCREF(&OneToManyDictType);
    PyModule_AddObject(m, "OneToManyDict", (PyObject*)&OneToManyDictType);

    return m;
}

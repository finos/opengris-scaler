#include "scaler/utility/indexed_queue.h"

#include "scaler/utility/pymod/compatibility.h"

namespace scaler {
namespace utility {
namespace indexed_queue {
namespace pymod {

using scaler::utility::pymod::OwnedPyObject;

extern "C" {
struct PyIndexedQueue {
    PyObject_HEAD;
    scaler::utility::IndexedQueue<OwnedPyObject<PyObject>> queue;
};

static PyObject* PyIndexedQueueNew(PyTypeObject* type, PyObject* args, PyObject* kwds)
{
    PyIndexedQueue* self {};
    self = (PyIndexedQueue*)type->tp_alloc(type, 0);
    return (PyObject*)self;
}

static int PyIndexedQueueInit(PyIndexedQueue* self, PyObject* args, PyObject* kwds)
{
    new (&((PyIndexedQueue*)self)->queue) scaler::utility::IndexedQueue<OwnedPyObject<PyObject>>();
    return 0;
}

static void PyIndexedQueueDealloc(PyObject* self)
{
    ((PyIndexedQueue*)self)->queue.~IndexedQueue<OwnedPyObject<PyObject>>();
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject* PyIndexedQueuePut(PyIndexedQueue* self, PyObject* args)
{
    PyObject* item {};

    if (!PyArg_ParseTuple(args, "O", &item)) {
        return nullptr;
    }

    const auto exists = self->queue.put(OwnedPyObject<>::fromBorrowed(item));
    if (!exists) {
        PyErr_SetString(PyExc_KeyError, "Items in IndexedQueue must be unique");
        return nullptr;
    }
    Py_RETURN_NONE;
}

static PyObject* PyIndexedQueueGet(PyIndexedQueue* self, PyObject* args)
{
    (void)args;

    auto [item, exists] = self->queue.get();
    if (!exists) {
        PyErr_SetString(PyExc_ValueError, "cannot get from an empty IndexedQueue");
        return nullptr;
    }

    return item.take();
}

static PyObject* PyIndexedQueueRemove(PyIndexedQueue* self, PyObject* args)
{
    PyObject* data {};
    if (!PyArg_ParseTuple(args, "O", &data)) {
        return nullptr;
    }

    const auto exists = self->queue.remove(OwnedPyObject<>::fromBorrowed(data));
    if (!exists) {
        PyErr_SetString(PyExc_ValueError, "Specified item not found in queue");
    }
    Py_RETURN_NONE;
}

static int PyIndexedQueueContains(PyObject* self, PyObject* item)
{
    return ((PyIndexedQueue*)self)->queue.contains(OwnedPyObject<>::fromBorrowed(item));
}

static Py_ssize_t PyIndexedQueueSize(PyObject* self)
{
    return ((PyIndexedQueue*)self)->queue.size();
}

static PyObject* PyIndexedQueueToList(PyIndexedQueue* self, PyObject* args)
{
    (void)args;

    auto& list       = self->queue._list;
    PyObject* pyList = PyList_New(static_cast<Py_ssize_t>(list.size()));
    if (!pyList) {
        return nullptr;
    }

    Py_ssize_t i = 0;
    for (auto it = list.rbegin(); it != list.rend(); ++it, ++i) {
        auto item = *it;
        PyList_SET_ITEM(pyList, i, item.take());
    }

    return pyList;
}

// Define the methods for the IndexedQueue Python class
static PyMethodDef PyIndexedQueueMethods[] = {
    {"__len__", (PyCFunction)PyIndexedQueueSize, METH_NOARGS, "__len__ method"},
    {"put", (PyCFunction)PyIndexedQueuePut, METH_VARARGS, "Put an item to IndexedQueue"},
    {"get", (PyCFunction)PyIndexedQueueGet, METH_VARARGS, "Pop and Return an item from IndexedQueue"},
    {"remove", (PyCFunction)PyIndexedQueueRemove, METH_VARARGS, "Remove an item from IndexedQueue"},
    {"to_list", (PyCFunction)PyIndexedQueueToList, METH_NOARGS, "Return the queue contents as a Python list"},
    {nullptr},
};

static PyType_Slot PyIndexedQueueSlots[] = {
    {Py_tp_new, (void*)PyIndexedQueueNew},
    {Py_tp_init, (void*)PyIndexedQueueInit},
    {Py_tp_dealloc, (void*)PyIndexedQueueDealloc},
    {Py_tp_methods, (void*)PyIndexedQueueMethods},
    {Py_sq_length, (void*)PyIndexedQueueSize},
    {Py_sq_contains, (void*)PyIndexedQueueContains},
    {Py_tp_doc, (void*)"IndexedQueue"},
    {0, nullptr},
};

static PyType_Spec PyIndexedQueueSpec = {
    .name      = "indexed_queue.IndexedQueue",
    .basicsize = sizeof(PyIndexedQueue),
    .itemsize  = 0,
    .flags     = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .slots     = PyIndexedQueueSlots,
};

static PyModuleDef indexed_queue_module = {
    .m_base  = PyModuleDef_HEAD_INIT,
    .m_name  = "indexed_queue",
    .m_doc   = PyDoc_STR("A module that wraps a C++ IndexedQueue class"),
    .m_size  = 0,
    .m_slots = nullptr,
    .m_free  = nullptr,
};
}
}  // namespace pymod
}  // namespace indexed_queue
}  // namespace utility
}  // namespace scaler

PyMODINIT_FUNC PyInit_indexed_queue(void)
{
    using scaler::utility::indexed_queue::pymod::indexed_queue_module;
    using scaler::utility::indexed_queue::pymod::PyIndexedQueueSpec;

    PyObject* m = PyModule_Create(&indexed_queue_module);
    if (!m) {
        return nullptr;
    }

    PyObject* type = PyType_FromSpec(&PyIndexedQueueSpec);
    if (!type) {
        Py_DECREF(m);
        return nullptr;
    }

    if (PyModule_AddObject(m, "IndexedQueue", type) < 0) {
        Py_DECREF(type);
        Py_DECREF(m);
        return nullptr;
    }

    return m;
}

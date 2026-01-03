#include "scaler/utility/stable_priority_queue.h"

#include "scaler/utility/pymod/compatibility.h"

extern "C" {
struct PyStablePriorityQueue {
    PyObject_HEAD;
    scaler::utility::StablePriorityQueue<OwnedPyObject<PyObject>> queue;
};

static PyObject* PyStablePriorityQueueNew(PyTypeObject* type, PyObject* args, PyObject* kwds)
{
    PyStablePriorityQueue* self {};
    self = (PyStablePriorityQueue*)type->tp_alloc(type, 0);
    return (PyObject*)self;
}

static int PyStablePriorityQueueInit(PyStablePriorityQueue* self, PyObject* args, PyObject* kwds)
{
    new (&((PyStablePriorityQueue*)self)->queue) scaler::utility::StablePriorityQueue<OwnedPyObject<PyObject>>();
    return 0;
}

static void PyStablePriorityQueueDealloc(PyObject* self)
{
    ((PyStablePriorityQueue*)self)->queue.~StablePriorityQueue<OwnedPyObject<PyObject>>();
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject* PyStablePriorityQueuePut(PyStablePriorityQueue* self, PyObject* args)
{
    int64_t priority {};
    PyObject* data {};

    if (!PyArg_ParseTuple(args, "LO", &priority, &data)) {
        return nullptr;
    }

    self->queue.put({priority, OwnedPyObject<>::fromBorrowed(data)});
    Py_RETURN_NONE;
}

static PyObject* PyStablePriorityQueueGet(PyStablePriorityQueue* self, PyObject* args)
{
    (void)args;

    auto [priorityAndData, exists] = self->queue.get();
    if (!exists) {
        PyErr_SetString(PyExc_ValueError, "cannot get from an empty queue");
        return nullptr;
    }

    auto [priority, data] = std::move(priorityAndData);
    OwnedPyObject<> res   = PyTuple_Pack(2, PyLong_FromLongLong(priority), data.take());
    if (!res) {
        return nullptr;
    }

    return res.take();
}

static PyObject* PyStablePriorityQueueRemove(PyStablePriorityQueue* self, PyObject* args)
{
    PyObject* data {};
    if (!PyArg_ParseTuple(args, "O", &data)) {
        return nullptr;
    }

    self->queue.remove(OwnedPyObject<>::fromBorrowed(data));
    Py_RETURN_NONE;
}

static PyObject* PyStablePriorityQueueDecreasePriority(PyStablePriorityQueue* self, PyObject* args)
{
    PyObject* data {};
    if (!PyArg_ParseTuple(args, "O", &data)) {
        return nullptr;
    }

    self->queue.decreasePriority(OwnedPyObject<>::fromBorrowed(data));

    Py_RETURN_NONE;
}

static PyObject* PyStablePriorityQueueMaxPriorityItem(PyStablePriorityQueue* self, PyObject* args)
{
    (void)args;

    auto [priorityAndData, exists] = self->queue.maxPriorityItem();
    if (!exists) {
        PyErr_SetString(PyExc_ValueError, "cannot return max priority item from empty queue");
        return nullptr;
    }

    auto [priority, data] = std::move(priorityAndData);
    OwnedPyObject<> res   = PyTuple_Pack(2, PyLong_FromLongLong(priority), data.take());
    if (!res) {
        return nullptr;
    }

    return res.take();
}

// Define the methods for the StablePriorityQueue Python class
static PyMethodDef PyStablePriorityQueueMethods[] = {
    {"put", (PyCFunction)PyStablePriorityQueuePut, METH_VARARGS, "Put a priority-item list to the queue"},
    {"get",
     (PyCFunction)PyStablePriorityQueueGet,
     METH_VARARGS,
     "Pop and Return priority-item list with the highest priority in the queue"},
    {"remove", (PyCFunction)PyStablePriorityQueueRemove, METH_VARARGS, "Remove an item from the queue"},
    {"decrease_priority",
     (PyCFunction)PyStablePriorityQueueDecreasePriority,
     METH_VARARGS,
     "Decrease priority of an item"},
    {"max_priority_item",
     (PyCFunction)PyStablePriorityQueueMaxPriorityItem,
     METH_VARARGS,
     "Return priority-item list with the highest priority in the queue"},
    {nullptr},
};

static Py_ssize_t PyStablePriorityQueueSize(PyObject* self)
{
    return ((PyStablePriorityQueue*)self)->queue.size();
}

static PySequenceMethods PyStablePriorityQueueSequenceMethods = {
    .sq_length = PyStablePriorityQueueSize,
};

// Define the Python Object Type for StablePriorityQueue
static PyTypeObject PyStablePriorityQueueType = {
    .tp_name        = "stable_priority_queue.StablePriorityQueue",
    .tp_basicsize   = sizeof(PyStablePriorityQueue),
    .tp_dealloc     = (destructor)PyStablePriorityQueueDealloc,
    .tp_as_sequence = &PyStablePriorityQueueSequenceMethods,
    .tp_flags       = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .tp_doc         = "StablePriorityQueue",
    .tp_methods     = PyStablePriorityQueueMethods,
    .tp_init        = (initproc)PyStablePriorityQueueInit,
    .tp_new         = PyStablePriorityQueueNew,
};

static PyModuleDef stable_priority_queue_module = {
    .m_base  = PyModuleDef_HEAD_INIT,
    .m_name  = "stable_priority_queue",
    .m_doc   = PyDoc_STR("A module that wraps a C++ StablePriorityQueue class"),
    .m_size  = 0,
    .m_slots = nullptr,
    .m_free  = nullptr,
};

// Module initialization function
PyMODINIT_FUNC PyInit_stable_priority_queue(void)
{
    PyObject* m {};

    if (PyType_Ready(&PyStablePriorityQueueType) < 0) {
        return nullptr;
    }

    m = PyModule_Create(&stable_priority_queue_module);
    if (!m) {
        return nullptr;
    }

    Py_INCREF(&PyStablePriorityQueueType);
    PyModule_AddObject(m, "StablePriorityQueue", (PyObject*)&PyStablePriorityQueueType);

    return m;
}
}

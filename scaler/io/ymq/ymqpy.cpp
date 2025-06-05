#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>

// C
#include <stddef.h>

#include <cstring>

// C++
#include <utility>
#include <vector>

// First-party
#include "bytes.hpp"
#include "io_socket.hpp"

struct YmqState {
    PyObject* enumModule;  // Reference to the enum module
};

struct PyBytesYmq {
    PyObject_HEAD;
    Bytes bytes;  // <- the actual bytes object
    int shares;
};

static PyObject* PyBytesYmq_repr(PyBytesYmq* self) {
    if (self->bytes.is_empty()) {
        return PyUnicode_FromString("<Bytes: empty>");
    } else {
        return PyUnicode_FromFormat("<Bytes: %db>", self->bytes.len());
    }
}

static PyObject* PyBytesYmq_data_getter(PyBytesYmq* self) {
    return PyBytes_FromStringAndSize((const char*)self->bytes.data(), self->bytes.len());
}

static PyObject* PyBytesYmq_len_getter(PyBytesYmq* self) {
    return PyLong_FromSize_t(self->bytes.len());
}

static PyGetSetDef PyBytesYmq_properties[] = {
    {"data", (getter)PyBytesYmq_data_getter, nullptr, PyDoc_STR("Data of the Bytes object"), nullptr},
    {"len", (getter)PyBytesYmq_len_getter, nullptr, PyDoc_STR("Length of the Bytes object"), nullptr},
    {nullptr, nullptr, nullptr, nullptr, nullptr}  // Sentinel
};

static int PyBytesYmq_getbuffer(PyBytesYmq* self, Py_buffer* view, int flags) {
    self->shares++;

    return PyBuffer_FillInfo(view, (PyObject*)self, (void*)self->bytes.data(), self->bytes.len(), true, flags);
}

static void PyBytesYmq_releasebuffer(PyBytesYmq* self, Py_buffer* view) {
    self->shares--;

    if (self->shares <= 0) {
        // TODO: free data here?
    }
}

static PyBufferProcs PyBytesYmqBufferProcs = {
    .bf_getbuffer     = (getbufferproc)PyBytesYmq_getbuffer,
    .bf_releasebuffer = (releasebufferproc)PyBytesYmq_releasebuffer,
};

static int PyBytesYmq_init(PyBytesYmq* self, PyObject* args, PyObject* kwds) {
    PyObject* bytes = nullptr;
    if (!PyArg_ParseTuple(args, "O", &bytes)) {
        return -1;  // Error parsing arguments
    }

    if (!PyBytes_Check(bytes)) {
        bytes = PyObject_Bytes(bytes);

        if (!bytes) {
            PyErr_SetString(PyExc_TypeError, "Expected bytes or bytes-like object");
            return -1;
        }
    }

    char* data;
    Py_ssize_t len;

    if (PyBytes_AsStringAndSize(bytes, &data, &len) < 0) {
        PyErr_SetString(PyExc_TypeError, "Failed to get bytes data");
        return -1;
    }

    self->bytes  = Bytes::copy((uint8_t*)data, len);
    self->shares = 0;

    return 0;
}

static void PyBytesYmq_dealloc(PyBytesYmq* self) {
    if (self->shares > 0) {
        PyErr_SetString(PyExc_RuntimeError, "Cannot deallocate Bytes while it is still shared");
        return;
    }

    self->bytes.~Bytes();  // Call the destructor of Bytes
    Py_TYPE(self)->tp_free((PyObject*)self);
}

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wreorder-init-list"
#pragma clang diagnostic ignored "-Wc99-designator"
// clang-format off
// this ordering is canonical as per the Python documentation
static PyTypeObject PyBytesYmqType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name      = "ymq.Bytes",
    .tp_doc       = PyDoc_STR("Bytes"),
    .tp_basicsize = sizeof(PyBytesYmq),
    .tp_itemsize  = 0,
    .tp_flags     = Py_TPFLAGS_DEFAULT,
    .tp_new       = PyType_GenericNew,
    .tp_init      = (initproc)PyBytesYmq_init,
    .tp_repr      = (reprfunc)PyBytesYmq_repr,
    .tp_dealloc   = (destructor)PyBytesYmq_dealloc,
    .tp_getset    = PyBytesYmq_properties,
    .tp_as_buffer = &PyBytesYmqBufferProcs,
};
// clang-format on
#pragma clang diagnostic pop

struct PyMessage {
    PyObject_HEAD;
    PyBytesYmq* address;
    PyBytesYmq* payload;
};

static int PyMessage_init(PyMessage* self, PyObject* args, PyObject* kwds) {
    PyBytesYmq *address = nullptr, *payload = nullptr;

    char* keywords[] = {const_cast<char*>("address"), const_cast<char*>("payload"), nullptr};

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "OO", keywords, address, payload)) {
        PyErr_SetString(PyExc_TypeError, "Expected two Bytes objects: address and payload");
        return -1;
    }

    if (!PyObject_TypeCheck((PyObject*)address, &PyBytesYmqType)) {
        PyErr_SetString(PyExc_TypeError, "Expected address to be a Bytes object");
        return -1;
    }

    if (!PyObject_TypeCheck((PyObject*)payload, &PyBytesYmqType)) {
        PyErr_SetString(PyExc_TypeError, "Expected payload to be a Bytes object");
        return -1;
    }

    self->address = address;
    self->payload = payload;
    Py_INCREF(self->address);  // Increment reference count for address
    Py_INCREF(self->payload);  // Increment reference count for payload

    return 0;
}

static void PyMessage_dealloc(PyMessage* self) {
    Py_DECREF(self->address);  // Decrement reference count for address
    Py_DECREF(self->payload);  // Decrement reference count for payload

    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject* PyMessage_repr(PyMessage* self) {
    return PyUnicode_FromFormat("<Message address=%R payload=%R>", self->address, self->payload);
}

static PyMemberDef PyMessage_members[] = {
    {"address", T_OBJECT_EX, offsetof(PyMessage, address), READONLY, PyDoc_STR("Address of the message")},
    {"payload", T_OBJECT_EX, offsetof(PyMessage, payload), READONLY, PyDoc_STR("Payload of the message")},
    {nullptr}  // Sentinel
};

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wreorder-init-list"
#pragma clang diagnostic ignored "-Wc99-designator"
// clang-format off
// this ordering is canonical as per the Python documentation
static PyTypeObject PyMessageType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name      = "ymq.Message",
    .tp_doc       = PyDoc_STR("Message"),
    .tp_basicsize = sizeof(PyMessage),
    .tp_itemsize  = 0,
    .tp_flags     = Py_TPFLAGS_DEFAULT,
    .tp_new       = PyType_GenericNew,
    .tp_init      = (initproc)PyMessage_init,
    .tp_dealloc   = (destructor)PyMessage_dealloc,
    .tp_repr      = (reprfunc)PyMessage_repr,
    .tp_members   = PyMessage_members,
};
// clang-format on
#pragma clang diagnostic pop

struct PyIoSocket {
    PyObject_HEAD;
    IOSocket* socket;  // <- the actual socket object
};

static int IoSocket_init(PyIoSocket* self, PyObject* args, PyObject* kwds) {
    char* identity = NULL;
    if (!PyArg_ParseTuple(args, "s", &identity))
        return -1;
    self->socket = nullptr;  // TODO!
    return 0;
}

static void IoSocket_dealloc(PyIoSocket* self) {
    self->socket->~IOSocket();
    self->socket = nullptr;

    Py_TYPE(self)->tp_free(self);
}

static PyObject* IoSocket_repr(PyIoSocket* self) {
    // return PyUnicode_FromFormat("<IoSocket>");
    return PyUnicode_FromString("<IoSocket>");
}

static PyObject* identity_getter(PyIoSocket* self, void* closure) {
    return PyUnicode_FromStringAndSize(self->socket->identity.data(), self->socket->identity.size());
}

static PyGetSetDef IoSocket_properties[] = {
    {"identity", (getter)identity_getter, nullptr, PyDoc_STR("Identity of the IoSocket"), nullptr},
    {nullptr, nullptr, nullptr, nullptr, nullptr}  // Sentinel
};

// static PyObject* IoSocket_send(PyIoSocket* self, PyObject)

static PyMethodDef IoSocket_methods[] = {
    // {"send", (PyCFunction)IoSocket_send, METH_VARARGS, PyDoc_STR("Send a message")},
    // {"recv", (PyCFunction)IoSocket_recv, METH_VARARGS, PyDoc_STR("Receive a message")},
    {nullptr, nullptr, 0, nullptr}  // Sentinel
};

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wreorder-init-list"
#pragma clang diagnostic ignored "-Wc99-designator"
// clang-format off
// this ordering is canonical as per the Python documentation
static PyTypeObject PyIoSocketType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name      = "ymq.IoSocket",
    .tp_doc       = PyDoc_STR("IoSocket"),
    .tp_basicsize = sizeof(PyIoSocket),
    .tp_itemsize  = 0,
    .tp_flags     = Py_TPFLAGS_DEFAULT,
    .tp_new       = PyType_GenericNew,
    .tp_init      = (initproc)IoSocket_init,
    .tp_repr      = (reprfunc)IoSocket_repr,
    .tp_dealloc   = (destructor)IoSocket_dealloc,
    .tp_getset    = IoSocket_properties,
    .tp_methods   = IoSocket_methods
};
// clang-format on
#pragma clang diagnostic pop

static int createIntEnum(PyObject* module, std::string enumName, std::vector<std::pair<std::string, int>> entries) {
    // create a python dictionary to hold the entries
    auto enumDict = PyDict_New();
    if (!enumDict) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create enum dictionary");
        return -1;
    }

    // add each entry to the dictionary
    for (const auto& entry: entries) {
        PyObject* value = PyLong_FromLong(entry.second);
        if (!value) {
            PyErr_SetString(PyExc_RuntimeError, "Failed to create enum value");
            Py_DECREF(enumDict);
            return -1;
        }

        if (PyDict_SetItemString(enumDict, entry.first.c_str(), value) < 0) {
            Py_DECREF(value);
            Py_DECREF(enumDict);
            PyErr_SetString(PyExc_RuntimeError, "Failed to set item in enum dictionary");
            return -1;
        }
        Py_DECREF(value);
    }

    auto state = (YmqState*)PyModule_GetState(module);

    if (!state) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to get module state");
        Py_DECREF(enumDict);
        return -1;
    }

    // create our class by calling enum.IntEnum(enumName, enumDict)
    auto enumClass = PyObject_CallMethod(state->enumModule, "IntEnum", "sO", enumName.c_str(), enumDict);
    Py_DECREF(enumDict);

    if (!enumClass) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create IntEnum class");
        return -1;
    }

    // add the class to the module
    // this increments the reference count of enumClass
    if (PyModule_AddObjectRef(module, enumName.c_str(), enumClass) < 0) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to add IntEnum class to module");
        Py_DECREF(enumClass);
        return -1;
    }
    Py_DECREF(enumClass);

    return 0;
}

static int createSocketTypesEnum(PyObject* module) {
    std::vector<std::pair<std::string, int>> socketTypes = {
        {
            "Binder",
            (int)SocketTypes::Binder,
        },
        {"Sub", (int)SocketTypes::Sub},
        {"Pub", (int)SocketTypes::Pub},
        {"Dealer", (int)SocketTypes::Dealer},
        {"Router", (int)SocketTypes::Router},
        {"Pair", (int)SocketTypes::Pair}};

    if (createIntEnum(module, "SocketTypes", socketTypes) < 0) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create SocketTypes enum");
        return -1;
    }

    return 0;
}

static int ymq_exec(PyObject* module) {
    if (PyType_Ready(&PyIoSocketType) < 0)
        return -1;

    if (PyModule_AddObjectRef(module, "IoSocket", (PyObject*)&PyIoSocketType) < 0)
        return -1;

    if (PyType_Ready(&PyBytesYmqType) < 0)
        return -1;

    if (PyModule_AddObjectRef(module, "Bytes", (PyObject*)&PyBytesYmqType) < 0)
        return -1;

    if (PyType_Ready(&PyMessageType) < 0)
        return -1;

    if (PyModule_AddObjectRef(module, "Message", (PyObject*)&PyMessageType) < 0)
        return -1;

    auto state = (YmqState*)PyModule_GetState(module);

    if (!state) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to get module state");
        return -1;
    }

    state->enumModule = PyImport_ImportModule("enum");

    if (!state->enumModule) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to import enum module");
        return -1;
    }

    if (createSocketTypesEnum(module) < 0)
        return -1;

    return 0;
}

static PyMethodDef ymq_methods[] = {
    {NULL, NULL, 0, NULL}  // Sentinel
};

static PyModuleDef_Slot ymq_slots[] = {
    {Py_mod_exec, (void*)ymq_exec},
    // only supported in Python 3.12+
    // {Py_mod_multiple_interpreters, Py_MOD_MULTIPLE_INTERPRETERS_NOT_SUPPORTED},
    {0, NULL}  // Sentinel
};

void ymq_free(YmqState* state) {
    Py_DECREF(state->enumModule);
    state->enumModule = nullptr;
}

static PyModuleDef ymq_module = {
    .m_base    = PyModuleDef_HEAD_INIT,
    .m_name    = "ymq",
    .m_doc     = PyDoc_STR("YMQ Python bindings"),
    .m_size    = sizeof(YmqState),
    .m_methods = ymq_methods,
    .m_slots   = ymq_slots,
    .m_free    = (freefunc)ymq_free,
};

PyMODINIT_FUNC PyInit_ymq(void) {
    return PyModuleDef_Init(&ymq_module);
}

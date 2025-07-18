#pragma once

// Python
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>

// C++
#include <chrono>
#include <latch>
#include <memory>
#include <thread>

// First-party
#include "scaler/io/ymq/bytes.h"
#include "scaler/io/ymq/io_context.h"
#include "scaler/io/ymq/io_socket.h"
#include "scaler/io/ymq/message.h"
#include "scaler/io/ymq/pymod_ymq/async.h"
#include "scaler/io/ymq/pymod_ymq/bytes.h"
#include "scaler/io/ymq/pymod_ymq/message.h"
#include "scaler/io/ymq/pymod_ymq/ymq.h"

using namespace scaler::ymq;

struct PyIOSocket {
    PyObject_HEAD;
    std::shared_ptr<IOSocket> socket;
    std::shared_ptr<IOContext> ioContext;
};

extern "C" {

static void PyIOSocket_dealloc(PyIOSocket* self) {
    self->ioContext->removeIOSocket(self->socket);
    self->ioContext.~shared_ptr();
    self->socket.~shared_ptr();
    Py_TYPE(self)->tp_free((PyObject*)self);  // Free the PyObject
}

static PyObject* PyIOSocket_send(PyIOSocket* self, PyObject* args, PyObject* kwargs) {
    PyMessage* message   = nullptr;
    const char* kwlist[] = {"message", nullptr};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O", (char**)kwlist, &message)) {
        Py_RETURN_NONE;
    }

    return async_wrapper((PyObject*)self, [&](YMQState* state, PyObject* future) {
        self->socket->sendMessage(
            {.address = std::move(message->address->bytes), .payload = std::move(message->payload->bytes)},
            [&](auto error) {
                // todo: after the core is updated
                // if (error) {
                //     future_raise_exception(
                //         future, [state, error]() { return YMQException_fromCoreException(state, &*error); });
                // } else {
                future_set_result(future, []() { Py_RETURN_NONE; });
                // }
            });
    });
}

static PyObject* PyIOSocket_send_sync(PyIOSocket* self, PyObject* args, PyObject* kwargs) {
    PyMessage* message   = nullptr;
    const char* kwlist[] = {"message", nullptr};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O", (char**)kwlist, &message)) {
        Py_RETURN_NONE;
    }

    std::latch waiter(1);
    int error = 0;

    self->socket->sendMessage(
        {.address = message->address ? std::move(message->address->bytes) : Bytes::empty(),
         .payload = std::move(message->payload->bytes)},
        [&](int e) {
            error = e;
            waiter.count_down();
        });

    // block the thread until the callback is called
    try {
        waiter.wait();
    } catch (const std::exception& e) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to bind to send synchronously");
        return nullptr;
    }

    if (error) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to send message synchronously");
        return nullptr;
    }

    Py_RETURN_NONE;
}

static PyObject* PyIOSocket_recv(PyIOSocket* self, PyObject* args) {
    return async_wrapper((PyObject*)self, [&](YMQState* state, PyObject* future) {
        self->socket->recvMessage([&](auto message) {
            future_set_result(future, [&]() {
                PyBytesYMQ* address = (PyBytesYMQ*)PyObject_CallNoArgs(state->PyBytesYMQType);
                if (!address) {
                    Py_RETURN_NONE;
                }

                PyBytesYMQ* payload = (PyBytesYMQ*)PyObject_CallNoArgs(state->PyBytesYMQType);
                if (!payload) {
                    Py_DECREF(address);
                    Py_RETURN_NONE;
                }

                address->bytes = std::move(message.address);
                payload->bytes = std::move(message.payload);

                PyMessage* message = (PyMessage*)PyObject_CallFunction(state->PyMessageType, "OO", address, payload);
                if (!message) {
                    Py_DECREF(address);
                    Py_DECREF(payload);
                    Py_RETURN_NONE;
                }

                return (PyObject*)message;
            });
        });
    });
}

static PyObject* PyIOSocket_recv_sync(PyIOSocket* self, PyObject* args) {
    // replace with PyType_GetModuleByDef(Py_TYPE(self), &ymq_module) in a newer Python version
    // https://docs.python.org/3/c-api/type.html#c.PyType_GetModuleByDef
    PyObject* pyModule = PyType_GetModule(Py_TYPE(self));
    if (!pyModule) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to get module for Message type");
        return nullptr;
    }

    auto state = (YMQState*)PyModule_GetState(pyModule);
    if (!state) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to get module state");
        return nullptr;
    }

    Message message;
    std::latch waiter(1);

    self->socket->recvMessage([&](auto _message) {
        message = std::move(_message);
        waiter.count_down();
    });

    // block the thread until the callback is called
    // block the thread until the callback is called
    try {
        waiter.wait();
    } catch (const std::exception& e) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to bind to recv synchronously");
        return nullptr;
    }

    PyBytesYMQ* address = (PyBytesYMQ*)PyObject_CallNoArgs(state->PyBytesYMQType);
    if (!address) {
        Py_RETURN_NONE;
    }

    PyBytesYMQ* payload = (PyBytesYMQ*)PyObject_CallNoArgs(state->PyBytesYMQType);
    if (!payload) {
        Py_DECREF(address);
        Py_RETURN_NONE;
    }

    address->bytes = std::move(message.address);
    payload->bytes = std::move(message.payload);

    PyMessage* pyMessage = (PyMessage*)PyObject_CallFunction(state->PyMessageType, "OO", address, payload);
    if (!pyMessage) {
        Py_DECREF(address);
        Py_DECREF(payload);
        Py_RETURN_NONE;
    }

    return (PyObject*)pyMessage;
}

static PyObject* PyIOSocket_bind(PyIOSocket* self, PyObject* args, PyObject* kwargs) {
    PyObject* addressObj;
    const char* kwlist[] = {"address", nullptr};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O", (char**)kwlist, &addressObj)) {
        PyErr_SetString(PyExc_TypeError, "expected one argument: address");
        Py_RETURN_NONE;
    }

    if (!PyUnicode_Check(addressObj)) {
        Py_DECREF(addressObj);

        PyErr_SetString(PyExc_TypeError, "argument must be a str");
        Py_RETURN_NONE;
    }

    Py_ssize_t addressLen;
    const char* address = PyUnicode_AsUTF8AndSize(addressObj, &addressLen);

    if (!address)
        Py_RETURN_NONE;

    return async_wrapper((PyObject*)self, [=](YMQState* state, PyObject* future) {
        self->socket->bindTo(std::string(address, addressLen), [=](auto error) {
            future_set_result(future, [=]() {
                if (error) {
                    PyErr_SetString(PyExc_RuntimeError, "Failed to bind to address");
                    return (PyObject*)nullptr;
                }

                Py_RETURN_NONE;
            });
        });
    });
}

static PyObject* PyIOSocket_bind_sync(PyIOSocket* self, PyObject* args, PyObject* kwargs) {
    PyObject* addressObj;
    const char* kwlist[] = {"address", nullptr};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O", (char**)kwlist, &addressObj)) {
        PyErr_SetString(PyExc_TypeError, "expected one argument: address");
        Py_RETURN_NONE;
    }

    if (!PyUnicode_Check(addressObj)) {
        Py_DECREF(addressObj);

        PyErr_SetString(PyExc_TypeError, "argument must be a str");
        Py_RETURN_NONE;
    }

    Py_ssize_t addressLen;
    const char* address = PyUnicode_AsUTF8AndSize(addressObj, &addressLen);

    if (!address)
        Py_RETURN_NONE;

    int error;
    std::latch waiter(1);

    self->socket->bindTo(std::string(address, addressLen), [&](int _error) {
        error = _error;
        waiter.count_down();
    });

    // block the thread until the callback is called
    try {
        waiter.wait();
    } catch (const std::exception& e) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to bind to address synchronously");
        return nullptr;
    }

    if (error) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to bind to address synchronously");
        return nullptr;
    }

    Py_RETURN_NONE;
}

static PyObject* PyIOSocket_connect(PyIOSocket* self, PyObject* args, PyObject* kwargs) {
    PyObject* addressObj;
    const char* kwlist[] = {"address", nullptr};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O", (char**)kwlist, &addressObj)) {
        PyErr_SetString(PyExc_TypeError, "expected one argument: address");
        Py_RETURN_NONE;
    }

    if (!PyUnicode_Check(addressObj)) {
        Py_DECREF(addressObj);

        PyErr_SetString(PyExc_TypeError, "argument must be a str");
        Py_RETURN_NONE;
    }

    Py_ssize_t addressLen;
    const char* address = PyUnicode_AsUTF8AndSize(addressObj, &addressLen);

    if (!address)
        Py_RETURN_NONE;

    return async_wrapper((PyObject*)self, [=](YMQState* state, PyObject* future) {
        self->socket->connectTo(std::string(address, addressLen), [=](auto error) {
            future_set_result(future, [=]() {
                if (error) {
                    PyErr_SetString(PyExc_RuntimeError, "Failed to connect to address");
                    return (PyObject*)nullptr;
                }

                Py_RETURN_NONE;
            });
        });
    });
}

static PyObject* PyIOSocket_connect_sync(PyIOSocket* self, PyObject* args, PyObject* kwargs) {
    PyObject* addressObj;
    const char* kwlist[] = {"address", nullptr};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O", (char**)kwlist, &addressObj)) {
        PyErr_SetString(PyExc_TypeError, "expected one argument: address");
        Py_RETURN_NONE;
    }

    if (!PyUnicode_Check(addressObj)) {
        Py_DECREF(addressObj);

        PyErr_SetString(PyExc_TypeError, "argument must be a str");
        Py_RETURN_NONE;
    }

    Py_ssize_t addressLen;
    const char* address = PyUnicode_AsUTF8AndSize(addressObj, &addressLen);

    if (!address)
        Py_RETURN_NONE;

    int error;
    std::latch waiter(1);

    self->socket->connectTo(std::string(address, addressLen), [&](int _error) {
        error = _error;
        waiter.count_down();
    });

    // block the thread until the callback is called
    try {
        waiter.wait();
    } catch (const std::exception& e) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to bind to connect synchronously");
        return nullptr;
    }

    if (error) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to connect to address synchronously");
        return nullptr;
    }

    Py_RETURN_NONE;
}

static PyObject* PyIOSocket_repr(PyIOSocket* self) {
    return PyUnicode_FromFormat("<IOSocket at %p>", (void*)self->socket.get());
}

static PyObject* PyIOSocket_identity_getter(PyIOSocket* self, void* closure) {
    return PyUnicode_FromStringAndSize(self->socket->identity().data(), self->socket->identity().size());
}

static PyObject* PyIOSocket_socket_type_getter(PyIOSocket* self, void* closure) {
    // replace with PyType_GetModuleByDef(Py_TYPE(self), &ymq_module) in a newer Python version
    // https://docs.python.org/3/c-api/type.html#c.PyType_GetModuleByDef
    PyObject* pyModule = PyType_GetModule(Py_TYPE(self));
    if (!pyModule) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to get module for Message type");
        return nullptr;
    }

    auto state = (YMQState*)PyModule_GetState(pyModule);
    if (!state) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to get module state");
        return nullptr;
    }

    IOSocketType socketType    = self->socket->socketType();
    PyObject* socketTypeIntObj = PyLong_FromLong((long)socketType);

    if (!socketTypeIntObj) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to convert socket type to a Python integer");
        return nullptr;
    }

    PyObject* socketTypeObj = PyObject_CallOneArg(state->PyIOSocketEnumType, socketTypeIntObj);
    Py_DECREF(socketTypeIntObj);

    if (!socketTypeObj) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create IOSocketType object");
        return nullptr;
    }

    return socketTypeObj;
}
}

static PyGetSetDef PyIOSocket_properties[] = {
    {"identity", (getter)PyIOSocket_identity_getter, nullptr, PyDoc_STR("Get the identity of the IOSocket"), nullptr},
    {"socket_type", (getter)PyIOSocket_socket_type_getter, nullptr, PyDoc_STR("Get the type of the IOSocket"), nullptr},
    {nullptr, nullptr, nullptr, nullptr, nullptr}};

static PyMethodDef PyIOSocket_methods[] = {
    {"send", (PyCFunction)PyIOSocket_send, METH_VARARGS | METH_KEYWORDS, PyDoc_STR("Send data through the IOSocket")},
    {"recv", (PyCFunction)PyIOSocket_recv, METH_NOARGS, PyDoc_STR("Receive data from the IOSocket")},
    {"bind",
     (PyCFunction)PyIOSocket_bind,
     METH_VARARGS | METH_KEYWORDS,
     PyDoc_STR("Bind to an address and listen for incoming connections")},
    {"connect",
     (PyCFunction)PyIOSocket_connect,
     METH_VARARGS | METH_KEYWORDS,
     PyDoc_STR("Connect to a remote IOSocket")},
    {"send_sync",
     (PyCFunction)PyIOSocket_send_sync,
     METH_VARARGS | METH_KEYWORDS,
     PyDoc_STR("Send data through the IOSocket synchronously")},
    {"recv_sync", (PyCFunction)PyIOSocket_recv_sync, METH_NOARGS, PyDoc_STR("Receive data from the IOSocket")},
    {"bind_sync",
     (PyCFunction)PyIOSocket_bind_sync,
     METH_VARARGS | METH_KEYWORDS,
     PyDoc_STR("Bind to an address and listen for incoming connections")},
    {"connect_sync",
     (PyCFunction)PyIOSocket_connect_sync,
     METH_VARARGS | METH_KEYWORDS,
     PyDoc_STR("Connect to a remote IOSocket")},
    {nullptr, nullptr, 0, nullptr}};

static PyType_Slot PyIOSocket_slots[] = {
    {Py_tp_dealloc, (void*)PyIOSocket_dealloc},
    {Py_tp_repr, (void*)PyIOSocket_repr},
    {Py_tp_getset, (void*)PyIOSocket_properties},
    {Py_tp_methods, (void*)PyIOSocket_methods},
    {0, nullptr},
};

static PyType_Spec PyIOSocket_spec = {
    .name      = "ymq.IOSocket",
    .basicsize = sizeof(PyIOSocket),
    .itemsize  = 0,
    .flags     = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_IMMUTABLETYPE | Py_TPFLAGS_DISALLOW_INSTANTIATION,
    .slots     = PyIOSocket_slots,
};

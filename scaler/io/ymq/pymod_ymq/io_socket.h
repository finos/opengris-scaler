#pragma once

// Python
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>

// C++
#include <chrono>
#include <expected>
#include <memory>
#include <thread>
#include <utility>

// C
#include <semaphore.h>
#include <sys/eventfd.h>
#include <sys/poll.h>

// First-party
#include "scaler/io/ymq/bytes.h"
#include "scaler/io/ymq/io_context.h"
#include "scaler/io/ymq/io_socket.h"
#include "scaler/io/ymq/message.h"
#include "scaler/io/ymq/pymod_ymq/async.h"
#include "scaler/io/ymq/pymod_ymq/bytes.h"
#include "scaler/io/ymq/pymod_ymq/exception.h"
#include "scaler/io/ymq/pymod_ymq/message.h"
#include "scaler/io/ymq/pymod_ymq/utils.h"
#include "scaler/io/ymq/pymod_ymq/ymq.h"

using namespace scaler::ymq;

struct PyIOSocket {
    PyObject_HEAD;
    std::shared_ptr<IOSocket> socket;
    std::shared_ptr<IOContext> ioContext;
};

extern "C" {

static void PyIOSocket_dealloc(PyIOSocket* self)
{
    try {
        self->ioContext->removeIOSocket(self->socket);
        self->ioContext.~shared_ptr();
        self->socket.~shared_ptr();
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to deallocate IOSocket");
        PyErr_WriteUnraisable((PyObject*)self);
    }

    auto* tp = Py_TYPE(self);
    tp->tp_free(self);
    Py_DECREF(tp);
}

static PyObject* PyIOSocket_send(PyIOSocket* self, PyObject* args, PyObject* kwargs)
{
    // borrowed reference
    PyMessage* message   = nullptr;
    const char* kwlist[] = {"message", nullptr};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O", (char**)kwlist, &message))
        return nullptr;

    auto address = message->address.is_none() ? Bytes() : std::move(message->address->bytes);
    auto payload = std::move(message->payload->bytes);

    return async_wrapper((PyObject*)self, [=](YMQState* state, auto future) {
        try {
            self->socket->sendMessage({.address = std::move(address), .payload = std::move(payload)}, [=](auto result) {
                future_set_result(future, [=] -> std::expected<PyObject*, PyObject*> {
                    if (result) {
                        Py_RETURN_NONE;
                    } else {
                        return std::unexpected {YMQException_createFromCoreError(state, &result.error())};
                    }
                });
            });
        } catch (...) {
            future_raise_exception(
                future, [] { return PyErr_CreateFromString(PyExc_RuntimeError, "Failed to send message"); });
        }
    });
}

static PyObject* PyIOSocket_send_sync(PyIOSocket* self, PyObject* args, PyObject* kwargs)
{
    auto state = YMQStateFromSelf((PyObject*)self);
    if (!state)
        return nullptr;

    // borrowed reference
    PyMessage* message   = nullptr;
    const char* kwlist[] = {"message", nullptr};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O", (char**)kwlist, &message))
        return nullptr;

    Bytes address = message->address.is_none() ? Bytes() : std::move(message->address->bytes);
    Bytes payload = std::move(message->payload->bytes);

    PyThreadState* _save = PyEval_SaveThread();

    std::shared_ptr<std::expected<void, Error>> result = std::make_shared<std::expected<void, Error>>();
    try {
        Waiter waiter(state->wakeupfd_rd);

        self->socket->sendMessage({.address = std::move(address), .payload = std::move(payload)}, [=](auto r) mutable {
            *result = std::move(r);
            waiter.signal();
        });

        if (waiter.wait())
            CHECK_SIGNALS;
    } catch (...) {
        PyEval_RestoreThread(_save);
        PyErr_SetString(PyExc_RuntimeError, "Failed to send synchronously");
        return nullptr;
    }

    PyEval_RestoreThread(_save);

    if (!result) {
        YMQException_setFromCoreError(state, &result->error());
        return nullptr;
    }

    Py_RETURN_NONE;
}

static PyObject* PyIOSocket_recv(PyIOSocket* self, PyObject* args)
{
    return async_wrapper((PyObject*)self, [=](YMQState* state, auto future) {
        self->socket->recvMessage([=](auto result) {
            try {
                future_set_result(future, [=] -> std::expected<PyObject*, PyObject*> {
                    if (result.second._errorCode != Error::ErrorCode::Uninit) {
                        return std::unexpected {YMQException_createFromCoreError(state, &result.second)};
                    }

                    auto message                      = result.first;
                    OwnedPyObject<PyBytesYMQ> address = (PyBytesYMQ*)PyObject_CallNoArgs(*state->PyBytesYMQType);
                    if (!address)
                        return YMQ_GetRaisedException();

                    address->bytes = std::move(message.address);

                    OwnedPyObject<PyBytesYMQ> payload = (PyBytesYMQ*)PyObject_CallNoArgs(*state->PyBytesYMQType);
                    if (!payload)
                        return YMQ_GetRaisedException();

                    payload->bytes = std::move(message.payload);

                    OwnedPyObject<PyMessage> pyMessage =
                        (PyMessage*)PyObject_CallFunction(*state->PyMessageType, "OO", *address, *payload);
                    if (!pyMessage)
                        return YMQ_GetRaisedException();

                    return (PyObject*)pyMessage.take();
                });
            } catch (...) {
                future_raise_exception(
                    future, [] { return PyErr_CreateFromString(PyExc_RuntimeError, "Failed to receive message"); });
            }
        });
    });
}

static PyObject* PyIOSocket_recv_sync(PyIOSocket* self, PyObject* args)
{
    auto state = YMQStateFromSelf((PyObject*)self);
    if (!state)
        return nullptr;

    PyThreadState* _save = PyEval_SaveThread();

    std::shared_ptr<std::pair<Message, Error>> result = std::make_shared<std::pair<Message, Error>>();
    try {
        Waiter waiter(state->wakeupfd_rd);

        self->socket->recvMessage([=](auto r) mutable {
            *result = std::move(r);
            waiter.signal();
        });

        if (waiter.wait())
            CHECK_SIGNALS;
    } catch (...) {
        PyEval_RestoreThread(_save);
        PyErr_SetString(PyExc_RuntimeError, "Failed to recv synchronously");
        return nullptr;
    }

    PyEval_RestoreThread(_save);

    if (result->second._errorCode != Error::ErrorCode::Uninit) {
        YMQException_setFromCoreError(state, &result->second);
        return nullptr;
    }

    auto message = result->first;

    OwnedPyObject<PyBytesYMQ> address = (PyBytesYMQ*)PyObject_CallNoArgs(*state->PyBytesYMQType);
    if (!address)
        return nullptr;

    address->bytes = std::move(message.address);

    OwnedPyObject<PyBytesYMQ> payload = (PyBytesYMQ*)PyObject_CallNoArgs(*state->PyBytesYMQType);
    if (!payload)
        return nullptr;

    payload->bytes = std::move(message.payload);

    OwnedPyObject<PyMessage> pyMessage =
        (PyMessage*)PyObject_CallFunction(*state->PyMessageType, "OO", *address, *payload);
    if (!pyMessage)
        return nullptr;

    return (PyObject*)pyMessage.take();
}

static PyObject* PyIOSocket_bind(PyIOSocket* self, PyObject* args, PyObject* kwargs)
{
    const char* address   = nullptr;
    Py_ssize_t addressLen = 0;
    const char* kwlist[]  = {"address", nullptr};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s#", (char**)kwlist, &address, &addressLen))
        return nullptr;

    return async_wrapper((PyObject*)self, [=](YMQState* state, auto future) {
        try {
            self->socket->bindTo(std::string(address, addressLen), [=](auto result) {
                future_set_result(future, [=] -> std::expected<PyObject*, PyObject*> {
                    if (!result) {
                        return std::unexpected {YMQException_createFromCoreError(state, &result.error())};
                    }

                    Py_RETURN_NONE;
                });
            });
        } catch (...) {
            future_raise_exception(
                future, [] { return PyErr_CreateFromString(PyExc_RuntimeError, "Failed to bind to address"); });
        }
    });
}

static PyObject* PyIOSocket_bind_sync(PyIOSocket* self, PyObject* args, PyObject* kwargs)
{
    auto state = YMQStateFromSelf((PyObject*)self);
    if (!state)
        return nullptr;

    const char* address   = nullptr;
    Py_ssize_t addressLen = 0;
    const char* kwlist[]  = {"address", nullptr};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s#", (char**)kwlist, &address, &addressLen))
        return nullptr;

    PyThreadState* _save = PyEval_SaveThread();

    auto result = std::make_shared<std::expected<void, Error>>();
    try {
        Waiter waiter(state->wakeupfd_rd);

        self->socket->bindTo(std::string(address, addressLen), [=](auto r) mutable {
            *result = std::move(r);
            waiter.signal();
        });

        if (waiter.wait())
            CHECK_SIGNALS;
    } catch (...) {
        PyEval_RestoreThread(_save);
        PyErr_SetString(PyExc_RuntimeError, "Failed to bind synchronously");
        return nullptr;
    }

    PyEval_RestoreThread(_save);

    if (!result) {
        YMQException_setFromCoreError(state, &result->error());
        return nullptr;
    }

    Py_RETURN_NONE;
}

static PyObject* PyIOSocket_connect(PyIOSocket* self, PyObject* args, PyObject* kwargs)
{
    const char* address   = nullptr;
    Py_ssize_t addressLen = 0;
    const char* kwlist[]  = {"address", nullptr};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s#", (char**)kwlist, &address, &addressLen))
        return nullptr;

    return async_wrapper((PyObject*)self, [=](YMQState* state, auto future) {
        try {
            self->socket->connectTo(std::string(address, addressLen), [=](auto result) {
                future_set_result(future, [=] -> std::expected<PyObject*, PyObject*> {
                    if (result || result.error()._errorCode == Error::ErrorCode::InitialConnectFailedWithInProgress) {
                        Py_RETURN_NONE;
                    } else {
                        return std::unexpected {YMQException_createFromCoreError(state, &result.error())};
                    }
                });
            });
        } catch (...) {
            future_raise_exception(
                future, [] { return PyErr_CreateFromString(PyExc_RuntimeError, "Failed to connect to address"); });
        }
    });
}

static PyObject* PyIOSocket_connect_sync(PyIOSocket* self, PyObject* args, PyObject* kwargs)
{
    auto state = YMQStateFromSelf((PyObject*)self);
    if (!state)
        return nullptr;

    const char* address   = nullptr;
    Py_ssize_t addressLen = 0;
    const char* kwlist[]  = {"address", nullptr};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s#", (char**)kwlist, &address, &addressLen))
        return nullptr;

    PyThreadState* _save = PyEval_SaveThread();

    std::shared_ptr<std::expected<void, Error>> result = std::make_shared<std::expected<void, Error>>();
    try {
        Waiter waiter(state->wakeupfd_rd);

        self->socket->connectTo(std::string(address, addressLen), [=](auto r) mutable {
            *result = std::move(r);
            waiter.signal();
        });

        if (waiter.wait())
            CHECK_SIGNALS;
    } catch (...) {
        PyEval_RestoreThread(_save);
        PyErr_SetString(PyExc_RuntimeError, "Failed to connect synchronously");
        return nullptr;
    }

    PyEval_RestoreThread(_save);

    if (!result && result->error()._errorCode != Error::ErrorCode::InitialConnectFailedWithInProgress) {
        YMQException_setFromCoreError(state, &result->error());
        return nullptr;
    }

    Py_RETURN_NONE;
}

static PyObject* PyIOSocket_repr(PyIOSocket* self)
{
    return PyUnicode_FromFormat("<IOSocket at %p>", (void*)self->socket.get());
}

static PyObject* PyIOSocket_identity_getter(PyIOSocket* self, void* closure)
{
    return PyUnicode_FromStringAndSize(self->socket->identity().data(), self->socket->identity().size());
}

static PyObject* PyIOSocket_socket_type_getter(PyIOSocket* self, void* closure)
{
    auto state = YMQStateFromSelf((PyObject*)self);
    if (!state)
        return nullptr;

    const IOSocketType socketType  = self->socket->socketType();
    OwnedPyObject socketTypeIntObj = PyLong_FromLong((long)socketType);

    if (!socketTypeIntObj)
        return nullptr;

    return PyObject_CallOneArg(*state->PyIOSocketEnumType, *socketTypeIntObj);
}
}

static PyGetSetDef PyIOSocket_properties[] = {
    {"identity", (getter)PyIOSocket_identity_getter, nullptr, PyDoc_STR("Get the identity of the IOSocket"), nullptr},
    {"socket_type", (getter)PyIOSocket_socket_type_getter, nullptr, PyDoc_STR("Get the type of the IOSocket"), nullptr},
    {nullptr, nullptr, nullptr, nullptr, nullptr},
};

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
    {nullptr, nullptr, 0, nullptr},
};

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

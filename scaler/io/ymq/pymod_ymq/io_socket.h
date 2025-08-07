#pragma once

// Python
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>

// C++
#include <chrono>
#include <expected>
#include <latch>
#include <memory>
#include <thread>
#include <utility>

// First-party
#include "scaler/io/ymq/bytes.h"
#include "scaler/io/ymq/io_context.h"
#include "scaler/io/ymq/io_socket.h"
#include "scaler/io/ymq/message.h"
#include "scaler/io/ymq/pymod_ymq/async.h"
#include "scaler/io/ymq/pymod_ymq/bytes.h"
#include "scaler/io/ymq/pymod_ymq/exception.h"
#include "scaler/io/ymq/pymod_ymq/message.h"
#include "scaler/io/ymq/pymod_ymq/ymq.h"

using namespace scaler::ymq;
using namespace std::chrono_literals;

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
        // set the error and continue to free the object
        PyErr_SetString(PyExc_RuntimeError, "Failed to deallocate IOSocket");
    }

    auto* tp = Py_TYPE(self);
    tp->tp_free(self);
    Py_DECREF(tp);
}

static PyObject* PyIOSocket_send(PyIOSocket* self, PyObject* args, PyObject* kwargs)
{
    PyMessage* message   = nullptr;
    const char* kwlist[] = {"message", nullptr};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O", (char**)kwlist, &message))
        return nullptr;

    return async_wrapper((PyObject*)self, [=](YMQState* state, PyObject* future) {
        try {
            self->socket->sendMessage(
                {.address = (PyObject*)message->address == Py_None ? Bytes {(char*)nullptr, 0} :
                                                                     std::move(message->address->bytes),
                 .payload = std::move(message->payload->bytes)},
                [=](auto result) {
                    if (result) {
                        future_set_result(future, [] { Py_RETURN_NONE; });
                    } else {
                        future_raise_exception(
                            future, [=] { return YMQException_createFromCoreError(state, &result.error()); });
                    }
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

    PyMessage* message   = nullptr;
    const char* kwlist[] = {"message", nullptr};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O", (char**)kwlist, &message))
        return nullptr;

    Bytes address =
        (PyObject*)message->address == Py_None ? Bytes {(char*)nullptr, 0} : std::move(message->address->bytes);
    Bytes payload = std::move(message->payload->bytes);

    PyThreadState* _save = PyEval_SaveThread();

    std::shared_ptr<std::expected<void, Error>> result = std::make_shared<std::expected<void, Error>>();
    try {
        std::shared_ptr<std::latch> waiter = std::make_shared<std::latch>(1);

        self->socket->sendMessage({.address = std::move(address), .payload = std::move(payload)}, [=](auto r) {
            *result = std::move(r);
            waiter->count_down();
        });

        // block the thread until the callback is called
        for (;;) {
            if (waiter->try_wait())
                break;

            PyEval_RestoreThread(_save);
            if (PyErr_CheckSignals() < 0)
                return nullptr;
            _save = PyEval_SaveThread();

            std::this_thread::sleep_for(10ms);
        }
    } catch (const std::exception& e) {
        PyEval_RestoreThread(_save);
        PyErr_SetString(PyExc_RuntimeError, "Failed to bind to send synchronously");
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
    return async_wrapper((PyObject*)self, [=](YMQState* state, PyObject* future) {
        self->socket->recvMessage([=](auto result) {
            try {
                if (result.second._errorCode == Error::ErrorCode::Uninit) {
                    auto message = result.first;
                    future_set_result(future, [=] {
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

                        PyMessage* message =
                            (PyMessage*)PyObject_CallFunction(state->PyMessageType, "OO", address, payload);
                        if (!message) {
                            Py_DECREF(address);
                            Py_DECREF(payload);
                            Py_RETURN_NONE;
                        }

                        return (PyObject*)message;
                    });
                } else {
                    future_raise_exception(
                        future, [=] { return YMQException_createFromCoreError(state, &result.second); });
                }
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
        std::shared_ptr<std::latch> waiter = std::make_shared<std::latch>(1);

        self->socket->recvMessage([=](auto r) {
            *result = std::move(r);
            waiter->count_down();
        });

        // block the thread until the callback is called
        for (;;) {
            if (waiter->try_wait())
                break;

            PyEval_RestoreThread(_save);
            if (PyErr_CheckSignals() < 0)
                return nullptr;
            _save = PyEval_SaveThread();

            std::this_thread::sleep_for(10ms);
        }
    } catch (const std::exception& e) {
        PyEval_RestoreThread(_save);
        PyErr_SetString(PyExc_RuntimeError, "Failed to bind to recv synchronously");
        return nullptr;
    }

    PyEval_RestoreThread(_save);

    if (result->second._errorCode != Error::ErrorCode::Uninit) {
        YMQException_setFromCoreError(state, &result->second);
        return nullptr;
    }

    auto message = result->first;

    PyBytesYMQ* address = (PyBytesYMQ*)PyObject_CallNoArgs(state->PyBytesYMQType);
    if (!address)
        return nullptr;

    PyBytesYMQ* payload = (PyBytesYMQ*)PyObject_CallNoArgs(state->PyBytesYMQType);
    if (!payload) {
        Py_DECREF(address);
        return nullptr;
    }

    address->bytes = std::move(message.address);
    payload->bytes = std::move(message.payload);

    PyMessage* pyMessage = (PyMessage*)PyObject_CallFunction(state->PyMessageType, "OO", address, payload);
    if (!pyMessage) {
        Py_DECREF(address);
        Py_DECREF(payload);
        return nullptr;
    }

    return (PyObject*)pyMessage;
}

static PyObject* PyIOSocket_bind(PyIOSocket* self, PyObject* args, PyObject* kwargs)
{
    const char* address   = nullptr;
    Py_ssize_t addressLen = 0;
    const char* kwlist[]  = {"address", nullptr};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s#", (char**)kwlist, &address, &addressLen))
        return nullptr;

    return async_wrapper((PyObject*)self, [=](YMQState* state, PyObject* future) {
        try {
            self->socket->bindTo(std::string(address, addressLen), [=](auto result) {
                if (result) {
                    future_set_result(future, [] { Py_RETURN_NONE; });
                } else {
                    future_raise_exception(
                        future, [=] { return YMQException_createFromCoreError(state, &result.error()); });
                }
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

    std::shared_ptr<std::expected<void, Error>> result = std::make_shared<std::expected<void, Error>>();
    try {
        std::shared_ptr<std::latch> waiter = std::make_shared<std::latch>(1);

        self->socket->bindTo(std::string(address, addressLen), [=](auto r) {
            *result = std::move(r);
            waiter->count_down();
        });

        // block the thread until the callback is called
        for (;;) {
            if (waiter->try_wait())
                break;

            PyEval_RestoreThread(_save);
            if (PyErr_CheckSignals() < 0)
                return nullptr;
            _save = PyEval_SaveThread();

            std::this_thread::sleep_for(10ms);
        }
    } catch (const std::exception& e) {
        PyEval_RestoreThread(_save);
        PyErr_SetString(PyExc_RuntimeError, "Failed to bind to address synchronously");
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

    return async_wrapper((PyObject*)self, [=](YMQState* state, PyObject* future) {
        try {
            self->socket->connectTo(std::string(address, addressLen), [=](auto result) {
                if (result || result.error()._errorCode == Error::ErrorCode::InitialConnectFailedWithInProgress) {
                    future_set_result(future, [] { Py_RETURN_NONE; });
                } else {
                    future_raise_exception(
                        future, [=] { return YMQException_createFromCoreError(state, &result.error()); });
                }
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
        std::shared_ptr<std::latch> waiter = std::make_shared<std::latch>(1);

        self->socket->connectTo(std::string(address, addressLen), [=](auto r) {
            *result = std::move(r);
            waiter->count_down();
        });

        // block the thread until the callback is called
        for (;;) {
            if (waiter->try_wait())
                break;

            PyEval_RestoreThread(_save);
            if (PyErr_CheckSignals() < 0)
                return nullptr;
            _save = PyEval_SaveThread();

            std::this_thread::sleep_for(10ms);
        }
    } catch (const std::exception& e) {
        PyEval_RestoreThread(_save);
        PyErr_SetString(PyExc_RuntimeError, "Failed to bind to connect synchronously");
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

    IOSocketType socketType    = self->socket->socketType();
    PyObject* socketTypeIntObj = PyLong_FromLong((long)socketType);

    if (!socketTypeIntObj)
        return nullptr;

    PyObject* socketTypeObj = PyObject_CallOneArg(state->PyIOSocketEnumType, socketTypeIntObj);
    Py_DECREF(socketTypeIntObj);

    return socketTypeObj;
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

#pragma once

// Python
#include "scaler/ymq/pymod_ymq/compatibility.h"

// C++
#include <memory>
#include <utility>

// First-party
#include "scaler/error/error.h"
#include "scaler/ymq/bytes.h"
#include "scaler/ymq/io_context.h"
#include "scaler/ymq/io_socket.h"
#include "scaler/ymq/message.h"
#include "scaler/ymq/pymod_ymq/bytes.h"
#include "scaler/ymq/pymod_ymq/exception.h"
#include "scaler/ymq/pymod_ymq/gil.h"
#include "scaler/ymq/pymod_ymq/message.h"
#include "scaler/ymq/pymod_ymq/ymq.h"

namespace scaler {
namespace ymq {

struct PyIOSocket {
    PyObject_HEAD;
    std::shared_ptr<IOSocket> socket;
    std::shared_ptr<IOContext> ioContext;
};

extern "C" {

static void PyIOSocket_dealloc(PyIOSocket* self)
{
    try {
        std::shared_ptr<IOSocket> ioSocket   = std::move(self->socket);
        std::shared_ptr<IOContext> ioContext = std::move(self->ioContext);

        // this function blocks so it's important to release the GIL
        Py_BEGIN_ALLOW_THREADS;
        ioContext->removeIOSocket(ioSocket);
        Py_END_ALLOW_THREADS;
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
    auto state = YMQStateFromSelf((PyObject*)self);
    if (!state)
        return nullptr;

    PyObject* callback = nullptr;
    PyMessage* message = nullptr;

    // empty str -> positional only
    const char* kwlist[] = {"", "message", nullptr};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO", (char**)kwlist, &callback, &message))
        return nullptr;

    if (!PyObject_TypeCheck(message, (PyTypeObject*)*state->PyMessageType)) {
        PyErr_SetString(PyExc_TypeError, "message must be a Message");
        return nullptr;
    }

    auto address = message->address.is_none() ? Bytes() : std::move(message->address->bytes);
    auto payload = std::move(message->payload->bytes);

    try {
        self->socket->sendMessage(
            {.address = std::move(address), .payload = std::move(payload)},
            [callback_ = OwnedPyObject<>::fromBorrowed(callback), state](auto result) {
                AcquireGIL _;

                // Redefine the callback to ensure it is destroyed before the GIL is released.
                OwnedPyObject callback = std::move(callback_);

                if (result) {
                    OwnedPyObject result = PyObject_CallFunctionObjArgs(*callback, Py_None, nullptr);
                } else {
                    OwnedPyObject obj = YMQException_createFromCoreError(state, &result.error());
                    OwnedPyObject _   = PyObject_CallFunctionObjArgs(*callback, *obj, nullptr);
                }
            });
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to send message");
        return nullptr;
    }

    Py_RETURN_NONE;
}

static PyObject* PyIOSocket_recv(PyIOSocket* self, PyObject* args, PyObject* kwargs)
{
    auto state = YMQStateFromSelf((PyObject*)self);
    if (!state)
        return nullptr;

    PyObject* callback   = nullptr;
    const char* kwlist[] = {"", nullptr};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O", (char**)kwlist, &callback))
        return nullptr;

    try {
        self->socket->recvMessage(
            [callback_ = OwnedPyObject<>::fromBorrowed(callback), state](std::pair<Message, Error> result) {
                AcquireGIL _;

                // Redefine the callback to ensure it is destroyed before the GIL is released.
                OwnedPyObject callback = std::move(callback_);

                if (result.second._errorCode != Error::ErrorCode::Uninit) {
                    OwnedPyObject obj = YMQException_createFromCoreError(state, &result.second);
                    OwnedPyObject _   = PyObject_CallFunctionObjArgs(*callback, *obj, nullptr);
                    return;
                }

                auto message                      = result.first;
                OwnedPyObject<PyBytesYMQ> address = (PyBytesYMQ*)PyObject_CallNoArgs(*state->PyBytesYMQType);
                if (!address) {
                    completeCallbackWithRaisedException(*callback);
                    return;
                }

                address->bytes = std::move(message.address);

                OwnedPyObject<PyBytesYMQ> payload = (PyBytesYMQ*)PyObject_CallNoArgs(*state->PyBytesYMQType);
                if (!payload) {
                    completeCallbackWithRaisedException(*callback);
                    return;
                }

                payload->bytes = std::move(message.payload);

                OwnedPyObject<PyMessage> pyMessage =
                    (PyMessage*)PyObject_CallFunction(*state->PyMessageType, "OO", *address, *payload);
                if (!pyMessage) {
                    completeCallbackWithRaisedException(*callback);
                    return;
                }

                OwnedPyObject _result = PyObject_CallFunctionObjArgs(*callback, *pyMessage, nullptr);
            });
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to receive message");
        return nullptr;
    }

    Py_RETURN_NONE;
}

static PyObject* PyIOSocket_bind(PyIOSocket* self, PyObject* args, PyObject* kwargs)
{
    auto state = YMQStateFromSelf((PyObject*)self);
    if (!state)
        return nullptr;

    PyObject* callback    = nullptr;
    const char* address   = nullptr;
    Py_ssize_t addressLen = 0;
    const char* kwlist[]  = {"", "address", nullptr};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "Os#", (char**)kwlist, &callback, &address, &addressLen))
        return nullptr;

    try {
        self->socket->bindTo(
            std::string(address, addressLen),
            [callback_ = OwnedPyObject<>::fromBorrowed(callback), state](auto result) {
                AcquireGIL _;

                // Redefine the callback to ensure it is destroyed before the GIL is released.
                OwnedPyObject callback = std::move(callback_);

                if (!result) {
                    OwnedPyObject exc = YMQException_createFromCoreError(state, &result.error());
                    OwnedPyObject _   = PyObject_CallFunctionObjArgs(*callback, *exc, nullptr);
                } else {
                    OwnedPyObject _ = PyObject_CallFunctionObjArgs(*callback, Py_None, nullptr);
                }
            });
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to bind to address");
        return nullptr;
    }

    Py_RETURN_NONE;
}

static PyObject* PyIOSocket_connect(PyIOSocket* self, PyObject* args, PyObject* kwargs)
{
    auto state = YMQStateFromSelf((PyObject*)self);
    if (!state)
        return nullptr;

    PyObject* callback    = nullptr;
    const char* address   = nullptr;
    Py_ssize_t addressLen = 0;
    const char* kwlist[]  = {"", "address", nullptr};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "Os#", (char**)kwlist, &callback, &address, &addressLen))
        return nullptr;

    try {
        self->socket->connectTo(
            std::string(address, addressLen),
            [callback_ = OwnedPyObject<>::fromBorrowed(callback), state](auto result) {
                AcquireGIL _;

                // Redefine the callback to ensure it is destroyed before the GIL is released.
                OwnedPyObject callback = std::move(callback_);

                if (result || result.error()._errorCode == Error::ErrorCode::InitialConnectFailedWithInProgress) {
                    OwnedPyObject _ = PyObject_CallFunctionObjArgs(*callback, Py_None, nullptr);
                } else {
                    OwnedPyObject exc = YMQException_createFromCoreError(state, &result.error());
                    OwnedPyObject _   = PyObject_CallFunctionObjArgs(*callback, *exc, nullptr);
                }
            });
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to connect to address");
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
    {"recv", (PyCFunction)PyIOSocket_recv, METH_VARARGS | METH_KEYWORDS, PyDoc_STR("Receive data from the IOSocket")},
    {"bind",
     (PyCFunction)PyIOSocket_bind,
     METH_VARARGS | METH_KEYWORDS,
     PyDoc_STR("Bind to an address and listen for incoming connections")},
    {"connect",
     (PyCFunction)PyIOSocket_connect,
     METH_VARARGS | METH_KEYWORDS,
     PyDoc_STR("Connect to a remote IOSocket")},
    {nullptr, nullptr, 0, nullptr},
};

static PyType_Slot PyIOSocket_slots[] = {
    {Py_tp_dealloc, (void*)PyIOSocket_dealloc},
    {Py_tp_repr, (void*)PyIOSocket_repr},
    {Py_tp_getset, (void*)PyIOSocket_properties},
    {Py_tp_methods, (void*)PyIOSocket_methods},
    {Py_tp_new, nullptr},
    {0, nullptr},
};

static PyType_Spec PyIOSocket_spec = {
    .name      = "_ymq.BaseIOSocket",
    .basicsize = sizeof(PyIOSocket),
    .itemsize  = 0,
    .flags     = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_IMMUTABLETYPE | Py_TPFLAGS_DISALLOW_INSTANTIATION,
    .slots     = PyIOSocket_slots,
};

}  // namespace ymq
}  // namespace scaler

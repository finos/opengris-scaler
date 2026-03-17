#pragma once

// Python
#include "scaler/utility/pymod/compatibility.h"

// C++
#include <future>
#include <memory>

// First-party
#include "scaler/error/error.h"
#include "scaler/utility/pymod/gil.h"
#include "scaler/ymq/connector_socket.h"
#include "scaler/ymq/io_context.h"
#include "scaler/ymq/message.h"
#include "scaler/ymq/pymod/address.h"
#include "scaler/ymq/pymod/bytes.h"
#include "scaler/ymq/pymod/exception.h"
#include "scaler/ymq/pymod/io_context.h"
#include "scaler/ymq/pymod/message.h"
#include "scaler/ymq/pymod/ymq.h"

namespace scaler {
namespace ymq {
namespace pymod {

using scaler::utility::pymod::AcquireGIL;
using scaler::utility::pymod::OwnedPyObject;

struct PyConnectorSocket {
    PyObject_HEAD;
    std::unique_ptr<ConnectorSocket> socket;
    std::shared_ptr<IOContext> ioContext;
};

static OwnedPyObject<PyConnectorSocket> PyConnectorSocket_new(YMQState* state)
{
    if (!state)
        return {};

    OwnedPyObject<PyConnectorSocket> self {
        PyObject_New(PyConnectorSocket, reinterpret_cast<PyTypeObject*>(*state->PyConnectorSocketType))};

    if (!self)
        return {};

    // Placement-new the C++ members
    new (&self->socket) std::unique_ptr<ConnectorSocket>();
    new (&self->ioContext) std::shared_ptr<IOContext>();

    return self;
}

static int PyConnectorSocket_init(PyConnectorSocket* self, PyObject* args, PyObject* kwds)
{
    PyErr_SetString(PyExc_TypeError, "Use ConnectorSocket.connect() or ConnectorSocket.bind() to create a socket");
    return -1;
}

static PyObject* PyConnectorSocket_connect(PyObject* cls, PyObject* args, PyObject* kwds)
{
    auto state = YMQStateFromType((PyObject*)cls);
    if (!state)
        return nullptr;

    PyObject* onConnectCallback  = nullptr;
    PyIOContext* pyIOContext     = nullptr;
    const char* identity         = nullptr;
    Py_ssize_t identityLen       = 0;
    const char* address          = nullptr;
    Py_ssize_t addressLen        = 0;
    unsigned long maxRetryTimes  = defaultClientMaxRetryTimes;
    unsigned long initRetryDelay = defaultClientInitRetryDelay.count();
    const char* kwlist[]         = {
        "callback", "context", "identity", "address", "max_retry_times", "init_retry_delay", nullptr};

    if (!PyArg_ParseTupleAndKeywords(
            args,
            kwds,
            "OO!s#s#|kk",
            (char**)kwlist,
            &onConnectCallback,
            (PyTypeObject*)*state->PyIOContextType,
            &pyIOContext,
            &identity,
            &identityLen,
            &address,
            &addressLen,
            &maxRetryTimes,
            &initRetryDelay))
        return nullptr;

    OwnedPyObject<PyConnectorSocket> self = PyConnectorSocket_new(state);

    if (!self)
        return nullptr;

    self->ioContext = pyIOContext->ioContext;

    try {
        self->socket = std::make_unique<ConnectorSocket>(ConnectorSocket::connect(
            *self->ioContext,
            Identity {identity, static_cast<size_t>(identityLen)},
            std::string {address, static_cast<size_t>(addressLen)},
            [callback_ = OwnedPyObject<>::fromBorrowed(onConnectCallback),
             state](std::expected<void, scaler::ymq::Error> result) {
                AcquireGIL _;

                // Redefine the callback to ensure it is destroyed before the GIL is released.
                OwnedPyObject callback = std::move(callback_);

                if (!result) {
                    completeCallbackWithCoreError(state, callback, result.error());
                    return;
                }

                completeCallback(callback, OwnedPyObject<>::none());
            },
            maxRetryTimes,
            std::chrono::milliseconds(initRetryDelay)));
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create ConnectorSocket");
        return nullptr;
    }

    return reinterpret_cast<PyObject*>(self.take());
}

static PyObject* PyConnectorSocket_bind(PyObject* cls, PyObject* args, PyObject* kwds)
{
    auto state = YMQStateFromType((PyObject*)cls);
    if (!state)
        return nullptr;

    PyObject* onBindCallback = nullptr;
    PyIOContext* pyIOContext = nullptr;
    const char* identity     = nullptr;
    Py_ssize_t identityLen   = 0;
    const char* address      = nullptr;
    Py_ssize_t addressLen    = 0;
    const char* kwlist[]     = {"callback", "context", "identity", "address", nullptr};

    if (!PyArg_ParseTupleAndKeywords(
            args,
            kwds,
            "OO!s#s#",
            (char**)kwlist,
            &onBindCallback,
            (PyTypeObject*)*state->PyIOContextType,
            &pyIOContext,
            &identity,
            &identityLen,
            &address,
            &addressLen))
        return nullptr;

    OwnedPyObject<PyConnectorSocket> self = PyConnectorSocket_new(state);

    if (!self)
        return nullptr;

    self->ioContext = pyIOContext->ioContext;

    try {
        self->socket = std::make_unique<ConnectorSocket>(ConnectorSocket::bind(
            *self->ioContext,
            Identity {identity, static_cast<size_t>(identityLen)},
            std::string {address, static_cast<size_t>(addressLen)},
            [callback_ = OwnedPyObject<>::fromBorrowed(onBindCallback),
             state](std::expected<Address, scaler::ymq::Error> result) {
                AcquireGIL _;

                // Redefine the callback to ensure it is destroyed before the GIL is released.
                OwnedPyObject callback = std::move(callback_);

                if (!result) {
                    completeCallbackWithCoreError(state, callback, result.error());
                    return;
                }

                OwnedPyObject pyAddress = PyAddress_fromAddress(state, *result);
                if (!pyAddress) {
                    completeCallbackWithRaisedException(callback);
                    return;
                }

                completeCallback(callback, pyAddress);
            }));
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create ConnectorSocket");
        return nullptr;
    }

    return reinterpret_cast<PyObject*>(self.take());
}

static void PyConnectorSocket_dealloc(PyConnectorSocket* self)
{
    try {
        if (self->socket) {
            std::promise<void> onShutdown;
            self->socket->shutdown([&onShutdown]() { onShutdown.set_value(); });

            // release the GIL until the socket is actually closed
            Py_BEGIN_ALLOW_THREADS;
            onShutdown.get_future().wait();
            Py_END_ALLOW_THREADS;
        }

        // Explicitly call destructors for placement-new'd members
        self->socket.reset();
        self->ioContext.reset();
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to deallocate ConnectorSocket");
        PyErr_WriteUnraisable((PyObject*)self);
    }

    auto* tp = Py_TYPE(self);
    tp->tp_free(self);
    Py_DECREF(tp);
}

static PyObject* PyConnectorSocket_send_message(PyConnectorSocket* self, PyObject* args, PyObject* kwargs)
{
    auto state = YMQStateFromSelf((PyObject*)self);
    if (!state)
        return nullptr;

    PyObject* callback      = nullptr;
    PyBytes* messagePayload = nullptr;
    const char* kwlist[]    = {"on_message_send", "message_payload", nullptr};

    if (!PyArg_ParseTupleAndKeywords(
            args, kwargs, "OO!", (char**)kwlist, &callback, (PyTypeObject*)*state->PyBytesType, &messagePayload))
        return nullptr;

    try {
        self->socket->sendMessage(
            std::move(messagePayload->bytes),
            [callback_ = OwnedPyObject<>::fromBorrowed(callback),
             state](std::expected<void, scaler::ymq::Error> result) {
                AcquireGIL _;

                // Redefine the callback to ensure it is destroyed before the GIL is released.
                OwnedPyObject callback = std::move(callback_);

                if (!result) {
                    completeCallbackWithCoreError(state, callback, result.error());
                    return;
                }

                completeCallback(callback, OwnedPyObject<>::none());
            });
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to send message");
        return nullptr;
    }

    Py_RETURN_NONE;
}

static PyObject* PyConnectorSocket_recv_message(PyConnectorSocket* self, PyObject* args, PyObject* kwargs)
{
    auto state = YMQStateFromSelf((PyObject*)self);
    if (!state)
        return nullptr;

    PyObject* callback   = nullptr;
    const char* kwlist[] = {"callback", nullptr};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O", (char**)kwlist, &callback))
        return nullptr;

    try {
        self->socket->recvMessage([callback_ = OwnedPyObject<>::fromBorrowed(callback),
                                   state](std::expected<scaler::ymq::Message, scaler::ymq::Error> result) {
            AcquireGIL _;

            // Redefine the callback to ensure it is destroyed before the GIL is released.
            OwnedPyObject callback = std::move(callback_);

            if (!result.has_value()) {
                completeCallbackWithCoreError(state, callback, result.error());
                return;
            }

            scaler::ymq::Message& message = result.value();

            OwnedPyObject<PyBytes> address = (PyBytes*)PyObject_CallNoArgs(*state->PyBytesType);
            if (!address) {
                completeCallbackWithRaisedException(callback);
                return;
            }

            address->bytes = std::move(message.address);

            OwnedPyObject<PyBytes> payload = (PyBytes*)PyObject_CallNoArgs(*state->PyBytesType);
            if (!payload) {
                completeCallbackWithRaisedException(callback);
                return;
            }

            payload->bytes = std::move(message.payload);

            OwnedPyObject pyMessage = PyObject_CallFunction(*state->PyMessageType, "OO", *address, *payload);
            if (!pyMessage) {
                completeCallbackWithRaisedException(callback);
                return;
            }

            completeCallback(callback, pyMessage);
        });
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to receive message");
        return nullptr;
    }

    Py_RETURN_NONE;
}

static PyObject* PyConnectorSocket_repr(PyConnectorSocket* self)
{
    return PyUnicode_FromFormat("<ConnectorSocket at %p>", (void*)self->socket.get());
}

static PyObject* PyConnectorSocket_identity_getter(PyConnectorSocket* self, void* Py_UNUSED(closure))
{
    const Identity& identity = self->socket->identity();
    return PyUnicode_FromStringAndSize(identity.data(), identity.size());
}

static PyGetSetDef PyConnectorSocket_properties[] = {
    {"identity", (getter)PyConnectorSocket_identity_getter, nullptr, nullptr, nullptr},
    {nullptr, nullptr, nullptr, nullptr, nullptr},
};

static PyMethodDef PyConnectorSocket_methods[] = {
    {"connect", (PyCFunction)PyConnectorSocket_connect, METH_CLASS | METH_VARARGS | METH_KEYWORDS, nullptr},
    {"bind", (PyCFunction)PyConnectorSocket_bind, METH_CLASS | METH_VARARGS | METH_KEYWORDS, nullptr},
    {"send_message", (PyCFunction)PyConnectorSocket_send_message, METH_VARARGS | METH_KEYWORDS, nullptr},
    {"recv_message", (PyCFunction)PyConnectorSocket_recv_message, METH_VARARGS | METH_KEYWORDS, nullptr},
    {nullptr, nullptr, 0, nullptr},
};

static PyType_Slot PyConnectorSocket_slots[] = {
    {Py_tp_init, (void*)PyConnectorSocket_init},
    {Py_tp_dealloc, (void*)PyConnectorSocket_dealloc},
    {Py_tp_repr, (void*)PyConnectorSocket_repr},
    {Py_tp_getset, (void*)PyConnectorSocket_properties},
    {Py_tp_methods, (void*)PyConnectorSocket_methods},
    {0, nullptr},
};

static PyType_Spec PyConnectorSocket_spec = {
    .name      = "_ymq.ConnectorSocket",
    .basicsize = sizeof(PyConnectorSocket),
    .itemsize  = 0,
    .flags     = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_IMMUTABLETYPE,
    .slots     = PyConnectorSocket_slots,
};

}  // namespace pymod
}  // namespace ymq
}  // namespace scaler

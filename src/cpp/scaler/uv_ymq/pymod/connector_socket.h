#pragma once

// Python
#include "scaler/utility/pymod/compatibility.h"

// C++
#include <future>
#include <memory>

// First-party
#include "scaler/error/error.h"
#include "scaler/utility/pymod/gil.h"
#include "scaler/uv_ymq/connector_socket.h"
#include "scaler/uv_ymq/io_context.h"
#include "scaler/uv_ymq/pymod/address.h"
#include "scaler/uv_ymq/pymod/bytes.h"
#include "scaler/uv_ymq/pymod/exception.h"
#include "scaler/uv_ymq/pymod/io_context.h"
#include "scaler/uv_ymq/pymod/message.h"
#include "scaler/uv_ymq/pymod/uv_ymq.h"
#include "scaler/ymq/message.h"

namespace scaler {
namespace uv_ymq {
namespace pymod {

using scaler::utility::pymod::AcquireGIL;
using scaler::utility::pymod::OwnedPyObject;

struct PyConnectorSocket {
    PyObject_HEAD;
    std::unique_ptr<ConnectorSocket> socket;
    std::shared_ptr<IOContext> ioContext;
};

static int PyConnectorSocket_init(PyConnectorSocket* self, PyObject* args, PyObject* kwds)
{
    auto state = UVYMQStateFromSelf((PyObject*)self);
    if (!state)
        return -1;

    PyObject* onConnectCallback  = nullptr;
    PyObject* pyContext          = nullptr;
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
            "OOs#s#|kk",
            (char**)kwlist,
            &onConnectCallback,
            &pyContext,
            &identity,
            &identityLen,
            &address,
            &addressLen,
            &maxRetryTimes,
            &initRetryDelay))
        return -1;

    if (!PyObject_TypeCheck(pyContext, (PyTypeObject*)*state->PyIOContextType)) {
        PyErr_SetString(PyExc_TypeError, "context must be an IOContext");
        return -1;
    }

    auto* pyIOContext = reinterpret_cast<PyIOContext*>(pyContext);

    try {
        self->ioContext = pyIOContext->ioContext;
        self->socket    = std::make_unique<ConnectorSocket>(
            *self->ioContext,
            Identity(identity, identityLen),
            std::string(address, addressLen),
            [callback_ = OwnedPyObject<>::fromBorrowed(onConnectCallback),
             state](std::expected<void, scaler::ymq::Error> result) {
                AcquireGIL _;

                // Redefine the callback to ensure it is destroyed before the GIL is released.
                OwnedPyObject callback = std::move(callback_);

                if (!result) {
                    OwnedPyObject exc            = UVYMQException_createFromCoreError(state, &result.error());
                    OwnedPyObject callbackResult = PyObject_CallFunctionObjArgs(*callback, *exc, nullptr);
                    if (!callbackResult) {
                        PyErr_WriteUnraisable(*callback);
                    }
                    return;
                }

                OwnedPyObject callbackResult = PyObject_CallFunctionObjArgs(*callback, Py_None, nullptr);
                if (!callbackResult) {
                    PyErr_WriteUnraisable(*callback);
                }
            },
            maxRetryTimes,
            std::chrono::milliseconds(initRetryDelay));
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create ConnectorSocket");
        return -1;
    }

    return 0;
}

static void PyConnectorSocket_dealloc(PyConnectorSocket* self)
{
    try {
        std::promise<void> onShutdown;
        self->socket->shutdown([&onShutdown]() { onShutdown.set_value(); });

        // release the GIL until the socket is actually closed
        Py_BEGIN_ALLOW_THREADS;
        onShutdown.get_future().wait();
        Py_END_ALLOW_THREADS;

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
    auto state = UVYMQStateFromSelf((PyObject*)self);
    if (!state)
        return nullptr;

    PyObject* callback      = nullptr;
    PyBytes* messagePayload = nullptr;
    const char* kwlist[]    = {"on_message_send", "message_payload", nullptr};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO", (char**)kwlist, &callback, &messagePayload))
        return nullptr;

    if (!PyObject_TypeCheck((PyObject*)messagePayload, (PyTypeObject*)*state->PyBytesType)) {
        PyErr_SetString(PyExc_TypeError, "message_payload must be a Bytes");
        return nullptr;
    }

    try {
        self->socket->sendMessage(
            std::move(messagePayload->bytes),
            [callback_ = OwnedPyObject<>::fromBorrowed(callback),
             state](std::expected<void, scaler::ymq::Error> result) {
                AcquireGIL _;

                // Redefine the callback to ensure it is destroyed before the GIL is released.
                OwnedPyObject callback = std::move(callback_);

                if (result) {
                    OwnedPyObject callbackResult = PyObject_CallFunctionObjArgs(*callback, Py_None, nullptr);
                    if (!callbackResult) {
                        PyErr_WriteUnraisable(*callback);
                    }
                } else {
                    OwnedPyObject exc            = UVYMQException_createFromCoreError(state, &result.error());
                    OwnedPyObject callbackResult = PyObject_CallFunctionObjArgs(*callback, *exc, nullptr);
                    if (!callbackResult) {
                        PyErr_WriteUnraisable(*callback);
                    }
                }
            });
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to send message");
        return nullptr;
    }

    Py_RETURN_NONE;
}

static PyObject* PyConnectorSocket_recv_message(PyConnectorSocket* self, PyObject* args, PyObject* kwargs)
{
    auto state = UVYMQStateFromSelf((PyObject*)self);
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
                OwnedPyObject exc = UVYMQException_createFromCoreError(state, &result.error());

                OwnedPyObject callbackResult = PyObject_CallFunctionObjArgs(*callback, *exc, nullptr);
                if (!callbackResult) {
                    PyErr_WriteUnraisable(*callback);
                }
                return;
            }

            scaler::ymq::Message& message = result.value();

            OwnedPyObject<PyBytes> address = (PyBytes*)PyObject_CallNoArgs(*state->PyBytesType);
            if (!address) {
                OwnedPyObject exception      = OwnedPyObject<>::none();
                OwnedPyObject callbackResult = PyObject_CallFunctionObjArgs(*callback, *exception, nullptr);
                if (!callbackResult) {
                    PyErr_WriteUnraisable(*callback);
                }
                return;
            }

            address->bytes = std::move(message.address);

            OwnedPyObject<PyBytes> payload = (PyBytes*)PyObject_CallNoArgs(*state->PyBytesType);
            if (!payload) {
                OwnedPyObject exception      = OwnedPyObject<>::none();
                OwnedPyObject callbackResult = PyObject_CallFunctionObjArgs(*callback, *exception, nullptr);
                if (!callbackResult) {
                    PyErr_WriteUnraisable(*callback);
                }
                return;
            }

            payload->bytes = std::move(message.payload);

            OwnedPyObject<PyMessage> pyMessage =
                (PyMessage*)PyObject_CallFunction(*state->PyMessageType, "OO", *address, *payload);
            if (!pyMessage) {
                OwnedPyObject exception      = OwnedPyObject<>::none();
                OwnedPyObject callbackResult = PyObject_CallFunctionObjArgs(*callback, *exception, nullptr);
                if (!callbackResult) {
                    PyErr_WriteUnraisable(*callback);
                }
                return;
            }

            OwnedPyObject callbackResult = PyObject_CallFunctionObjArgs(*callback, *pyMessage, nullptr);
            if (!callbackResult) {
                PyErr_WriteUnraisable(*callback);
            }
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
    .name      = "_uv_ymq.ConnectorSocket",
    .basicsize = sizeof(PyConnectorSocket),
    .itemsize  = 0,
    .flags     = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_IMMUTABLETYPE,
    .slots     = PyConnectorSocket_slots,
};

}  // namespace pymod
}  // namespace uv_ymq
}  // namespace scaler

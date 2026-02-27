#pragma once

// Python
#include "scaler/utility/pymod/compatibility.h"

// C++
#include <future>
#include <memory>

// First-party
#include "scaler/error/error.h"
#include "scaler/utility/pymod/gil.h"
#include "scaler/uv_ymq/binder_socket.h"
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

struct PyBinderSocket {
    PyObject_HEAD;
    std::unique_ptr<BinderSocket> socket;
    std::shared_ptr<IOContext> ioContext;
};

static int PyBinderSocket_init(PyBinderSocket* self, PyObject* args, PyObject* kwds)
{
    auto state = UVYMQStateFromSelf((PyObject*)self);
    if (!state)
        return -1;

    PyObject* pyContext    = nullptr;
    const char* identity   = nullptr;
    Py_ssize_t identityLen = 0;
    const char* kwlist[]   = {"context", "identity", nullptr};

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "Os#", (char**)kwlist, &pyContext, &identity, &identityLen))
        return -1;

    if (!PyObject_TypeCheck(pyContext, (PyTypeObject*)*state->PyIOContextType)) {
        PyErr_SetString(PyExc_TypeError, "context must be an IOContext");
        return -1;
    }

    auto* pyIOContext = reinterpret_cast<PyIOContext*>(pyContext);

    try {
        self->ioContext = pyIOContext->ioContext;
        self->socket    = std::make_unique<BinderSocket>(*self->ioContext, std::string(identity, identityLen));
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create BinderSocket");
        return -1;
    }

    return 0;
}

static void PyBinderSocket_dealloc(PyBinderSocket* self)
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
        PyErr_SetString(PyExc_RuntimeError, "Failed to deallocate BinderSocket");
        PyErr_WriteUnraisable((PyObject*)self);
    }

    auto* tp = Py_TYPE(self);
    tp->tp_free(self);
    Py_DECREF(tp);
}

static PyObject* PyBinderSocket_bind_to(PyBinderSocket* self, PyObject* args, PyObject* kwargs)
{
    auto state = UVYMQStateFromSelf((PyObject*)self);
    if (!state)
        return nullptr;

    PyObject* callback    = nullptr;
    const char* address   = nullptr;
    Py_ssize_t addressLen = 0;
    const char* kwlist[]  = {"callback", "address", nullptr};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "Os#", (char**)kwlist, &callback, &address, &addressLen))
        return nullptr;

    try {
        self->socket->bindTo(
            std::string(address, addressLen),
            [callback_ = OwnedPyObject<>::fromBorrowed(callback),
             state](std::expected<Address, scaler::ymq::Error> result) {
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

                OwnedPyObject<PyAddress> pyAddress = (PyAddress*)PyAddress_fromAddress(state, *result);
                if (!pyAddress) {
                    OwnedPyObject exception      = OwnedPyObject<>::none();
                    OwnedPyObject callbackResult = PyObject_CallFunctionObjArgs(*callback, *exception, nullptr);
                    if (!callbackResult) {
                        PyErr_WriteUnraisable(*callback);
                    }
                    return;
                }

                OwnedPyObject callbackResult = PyObject_CallFunctionObjArgs(*callback, *pyAddress, nullptr);
                if (!callbackResult) {
                    PyErr_WriteUnraisable(*callback);
                }
            });
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to bind to address");
        return nullptr;
    }

    Py_RETURN_NONE;
}

static PyObject* PyBinderSocket_send_message(PyBinderSocket* self, PyObject* args, PyObject* kwargs)
{
    auto state = UVYMQStateFromSelf((PyObject*)self);
    if (!state)
        return nullptr;

    PyObject* callback           = nullptr;
    const char* remoteIdentity   = nullptr;
    Py_ssize_t remoteIdentityLen = 0;
    PyBytes* messagePayload      = nullptr;
    const char* kwlist[]         = {"on_message_send", "remote_identity", "message_payload", nullptr};

    if (!PyArg_ParseTupleAndKeywords(
            args, kwargs, "Os#O", (char**)kwlist, &callback, &remoteIdentity, &remoteIdentityLen, &messagePayload))
        return nullptr;

    if (!PyObject_TypeCheck((PyObject*)messagePayload, (PyTypeObject*)*state->PyBytesType)) {
        PyErr_SetString(PyExc_TypeError, "message_payload must be a Bytes");
        return nullptr;
    }

    try {
        self->socket->sendMessage(
            std::string(remoteIdentity, remoteIdentityLen),
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

static PyObject* PyBinderSocket_recv_message(PyBinderSocket* self, PyObject* args, PyObject* kwargs)
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

static PyObject* PyBinderSocket_close_connection(PyBinderSocket* self, PyObject* args, PyObject* kwargs)
{
    const char* remoteIdentity   = nullptr;
    Py_ssize_t remoteIdentityLen = 0;
    const char* kwlist[]         = {"remote_identity", nullptr};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s#", (char**)kwlist, &remoteIdentity, &remoteIdentityLen))
        return nullptr;

    try {
        self->socket->closeConnection(std::string(remoteIdentity, remoteIdentityLen));
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to close connection");
        return nullptr;
    }

    Py_RETURN_NONE;
}

static PyObject* PyBinderSocket_repr(PyBinderSocket* self)
{
    return PyUnicode_FromFormat("<BinderSocket at %p>", (void*)self->socket.get());
}

static PyObject* PyBinderSocket_identity_getter(PyBinderSocket* self, void* Py_UNUSED(closure))
{
    const Identity& identity = self->socket->identity();
    return PyUnicode_FromStringAndSize(identity.data(), identity.size());
}

static PyGetSetDef PyBinderSocket_properties[] = {
    {"identity", (getter)PyBinderSocket_identity_getter, nullptr, nullptr, nullptr},
    {nullptr, nullptr, nullptr, nullptr, nullptr},
};

static PyMethodDef PyBinderSocket_methods[] = {
    {"bind_to", (PyCFunction)PyBinderSocket_bind_to, METH_VARARGS | METH_KEYWORDS, nullptr},
    {"send_message", (PyCFunction)PyBinderSocket_send_message, METH_VARARGS | METH_KEYWORDS, nullptr},
    {"recv_message", (PyCFunction)PyBinderSocket_recv_message, METH_VARARGS | METH_KEYWORDS, nullptr},
    {"close_connection", (PyCFunction)PyBinderSocket_close_connection, METH_VARARGS | METH_KEYWORDS, nullptr},
    {nullptr, nullptr, 0, nullptr},
};

static PyType_Slot PyBinderSocket_slots[] = {
    {Py_tp_init, (void*)PyBinderSocket_init},
    {Py_tp_dealloc, (void*)PyBinderSocket_dealloc},
    {Py_tp_repr, (void*)PyBinderSocket_repr},
    {Py_tp_getset, (void*)PyBinderSocket_properties},
    {Py_tp_methods, (void*)PyBinderSocket_methods},
    {0, nullptr},
};

static PyType_Spec PyBinderSocket_spec = {
    .name      = "_uv_ymq.BinderSocket",
    .basicsize = sizeof(PyBinderSocket),
    .itemsize  = 0,
    .flags     = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_IMMUTABLETYPE,
    .slots     = PyBinderSocket_slots,
};

}  // namespace pymod
}  // namespace uv_ymq
}  // namespace scaler

#pragma once

// Python
#include "scaler/utility/pymod/compatibility.h"

// C++
#include <optional>
#include <string>
#include <utility>

// First-party
#include "scaler/ymq/pymod/ymq.h"
#include "scaler/ymq/tls_config.h"

namespace scaler {
namespace ymq {
namespace pymod {

struct PyTLSConfig {
    PyObject_HEAD;
    scaler::ymq::TLSConfig tlsConfig;
};

static int PyTLSConfig_init(PyTLSConfig* self, PyObject* args, PyObject* kwds)
{
    const char* certChain    = nullptr;
    Py_ssize_t certChainLen  = 0;
    const char* privateKey   = nullptr;
    Py_ssize_t privateKeyLen = 0;
    const char* kwlist[]     = {"cert_chain", "private_key", nullptr};

    if (!PyArg_ParseTupleAndKeywords(
            args, kwds, "s#s#", (char**)kwlist, &certChain, &certChainLen, &privateKey, &privateKeyLen)) {
        return -1;
    }

    try {
        new (&self->tlsConfig) scaler::ymq::TLSConfig(
            std::string {certChain, static_cast<size_t>(certChainLen)},
            std::string {privateKey, static_cast<size_t>(privateKeyLen)});
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create TLSConfig");
        return -1;
    }

    return 0;
}

static void PyTLSConfig_dealloc(PyTLSConfig* self)
{
    try {
        self->tlsConfig.~TLSConfig();
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to deallocate TLSConfig");
        PyErr_WriteUnraisable((PyObject*)self);
    }

    auto* tp = Py_TYPE(self);
    tp->tp_free(self);
    Py_DECREF(tp);
}

static PyObject* PyTLSConfig_repr(PyTLSConfig* self)
{
    return PyUnicode_FromFormat("<TLSConfig at %p>", (void*)self);
}

static PyType_Slot PyTLSConfig_slots[] = {
    {Py_tp_init, (void*)PyTLSConfig_init},
    {Py_tp_dealloc, (void*)PyTLSConfig_dealloc},
    {Py_tp_repr, (void*)PyTLSConfig_repr},
    {0, nullptr},
};

static PyType_Spec PyTLSConfig_spec = {
    .name      = "_ymq.TLSConfig",
    .basicsize = sizeof(PyTLSConfig),
    .itemsize  = 0,
    .flags     = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_IMMUTABLETYPE,
    .slots     = PyTLSConfig_slots,
};

static std::optional<scaler::ymq::TLSConfig> fromPyTLSConfig(PyObject* pyTLSConfig)
{
    if (pyTLSConfig == nullptr || pyTLSConfig == Py_None) {
        return std::nullopt;
    }

    return reinterpret_cast<PyTLSConfig*>(pyTLSConfig)->tlsConfig;
}

}  // namespace pymod
}  // namespace ymq
}  // namespace scaler

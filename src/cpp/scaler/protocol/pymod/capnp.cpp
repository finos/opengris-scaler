#include <Python.h>

#include <new>

#include "scaler/protocol/pymod/bootstrap.h"
#include "scaler/protocol/pymod/module_state.h"
#include "scaler/protocol/pymod/serialize.h"
#include "scaler/utility/pymod/compatibility.h"

namespace scaler::protocol::pymod {

int capnp_module_traverse(PyObject* module, visitproc visit, void* arg)
{ return traverse_module_state(module, visit, arg); }

int capnp_module_clear(PyObject* module)
{ return clear_module_state(module); }

void capnp_module_free(void* module)
{ clear_module_state(static_cast<PyObject*>(module)); }

PyModuleDef MODULE_DEF;

}  // namespace scaler::protocol::pymod

namespace {

PyObject* py_get_module_descriptor(PyObject* /*self*/, PyObject* args)
{
    const char* module_name = nullptr;
    if (!PyArg_ParseTuple(args, "s", &module_name)) {
        return nullptr;
    }

    return scaler::protocol::pymod::get_module_descriptor(module_name).take();
}

PyObject* py_message_to_bytes(PyObject* /*self*/, PyObject* args)
{
    const char* variant_name = nullptr;
    PyObject* inner          = nullptr;
    if (!PyArg_ParseTuple(args, "sO", &variant_name, &inner)) {
        return nullptr;
    }
    return scaler::protocol::pymod::message_to_bytes(variant_name, inner).take();
}

PyObject* py_message_from_bytes(PyObject* /*self*/, PyObject* args)
{
    PyObject* data                     = nullptr;
    unsigned long long traversal_limit = (1ull << 63);
    if (!PyArg_ParseTuple(args, "O|K", &data, &traversal_limit)) {
        return nullptr;
    }
    return scaler::protocol::pymod::message_from_bytes(data, traversal_limit).take();
}

PyObject* py_struct_to_bytes(PyObject* /*self*/, PyObject* args)
{
    const char* type_name = nullptr;
    PyObject* obj         = nullptr;
    if (!PyArg_ParseTuple(args, "sO", &type_name, &obj)) {
        return nullptr;
    }
    return scaler::protocol::pymod::struct_to_bytes(type_name, obj).take();
}

PyObject* py_struct_from_bytes(PyObject* /*self*/, PyObject* args)
{
    const char* type_name              = nullptr;
    PyObject* data                     = nullptr;
    unsigned long long traversal_limit = (1ull << 63);
    if (!PyArg_ParseTuple(args, "sO|K", &type_name, &data, &traversal_limit)) {
        return nullptr;
    }
    return scaler::protocol::pymod::struct_from_bytes(type_name, data, traversal_limit).take();
}

static PyMethodDef MODULE_METHODS[] = {
    {"get_module_descriptor", py_get_module_descriptor, METH_VARARGS, nullptr},
    {"message_to_bytes", py_message_to_bytes, METH_VARARGS, nullptr},
    {"message_from_bytes", py_message_from_bytes, METH_VARARGS, nullptr},
    {"struct_to_bytes", py_struct_to_bytes, METH_VARARGS, nullptr},
    {"struct_from_bytes", py_struct_from_bytes, METH_VARARGS, nullptr},
    {nullptr, nullptr, 0, nullptr},
};

}  // namespace

PyMODINIT_FUNC PyInit_capnp(void)
{
    using scaler::utility::pymod::OwnedPyObject;

    scaler::protocol::pymod::MODULE_DEF = {
        PyModuleDef_HEAD_INIT,
        "capnp",
        nullptr,
        sizeof(scaler::protocol::pymod::CapnpModuleState),
        MODULE_METHODS,
        nullptr,
        scaler::protocol::pymod::capnp_module_traverse,
        scaler::protocol::pymod::capnp_module_clear,
        scaler::protocol::pymod::capnp_module_free,
    };

    OwnedPyObject<> module {PyModule_Create(&scaler::protocol::pymod::MODULE_DEF)};
    if (!module) {
        return nullptr;
    }

    auto* state = scaler::protocol::pymod::get_module_state(module.get());
    if (!state) {
        PyErr_SetString(PyExc_RuntimeError, "failed to allocate capnp module state");
        return nullptr;
    }
    new (state) scaler::protocol::pymod::CapnpModuleState {};

    scaler::protocol::pymod::set_initializing_module(module.get());
    if (!scaler::protocol::pymod::initialize_runtime_modules(module.get())) {
        scaler::protocol::pymod::set_initializing_module(nullptr);
        if (!PyErr_Occurred()) {
            PyErr_SetString(PyExc_RuntimeError, "failed to initialize capnp runtime modules");
        }
        return nullptr;
    }
    scaler::protocol::pymod::set_initializing_module(nullptr);

    return module.take();
}

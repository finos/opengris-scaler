#include <Python.h>

#include <new>

#include "scaler/protocol/pymod/bootstrap.h"
#include "scaler/protocol/pymod/module_state.h"
#include "scaler/utility/pymod/compatibility.h"

namespace scaler::protocol::pymod {

int capnp_module_traverse(PyObject* module, visitproc visit, void* arg)
{
    return traverse_module_state(module, visit, arg);
}

int capnp_module_clear(PyObject* module)
{
    return clear_module_state(module);
}

void capnp_module_free(void* module)
{
    clear_module_state(static_cast<PyObject*>(module));
}

PyModuleDef MODULE_DEF;

}  // namespace scaler::protocol::pymod

PyMODINIT_FUNC PyInit_capnp(void)
{
    using scaler::utility::pymod::OwnedPyObject;

    scaler::protocol::pymod::MODULE_DEF = {
        PyModuleDef_HEAD_INIT,
        "capnp",
        nullptr,
        sizeof(scaler::protocol::pymod::CapnpModuleState),
        nullptr,
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

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

    // The module name is bound to a named static array (not passed as an inline
    // string literal) because Pyodide's SIDE_MODULE wasm relocator can mis-resolve
    // offsets within mergeable .rodata.str sections, causing the module name to be
    // truncated. The named array is what matters, not the initializer spelling.
    static const char MODULE_NAME[]     = "capnp";
    scaler::protocol::pymod::MODULE_DEF = {
        PyModuleDef_HEAD_INIT,
        MODULE_NAME,
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
        static const char ERR[] = "failed to allocate capnp module state";
        PyErr_SetString(PyExc_RuntimeError, ERR);
        return nullptr;
    }
    new (state) scaler::protocol::pymod::CapnpModuleState {};

    scaler::protocol::pymod::set_initializing_module(module.get());
    if (!scaler::protocol::pymod::initialize_runtime_modules(module.get())) {
        scaler::protocol::pymod::set_initializing_module(nullptr);
        if (!PyErr_Occurred()) {
            static const char ERR[] = "failed to initialize capnp runtime modules";
            PyErr_SetString(PyExc_RuntimeError, ERR);
        }
        return nullptr;
    }
    scaler::protocol::pymod::set_initializing_module(nullptr);

    return module.take();
}

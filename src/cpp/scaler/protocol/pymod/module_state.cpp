#include "scaler/protocol/pymod/module_state.h"

namespace scaler::protocol::pymod {

extern PyModuleDef MODULE_DEF;

namespace {

PyObject* INITIALIZING_MODULE = nullptr;

}

CapnpModuleState* get_module_state()
{
    PyObject* module = PyState_FindModule(&MODULE_DEF);
    if (!module) {
        module = INITIALIZING_MODULE;
    }
    if (!module) {
        return nullptr;
    }
    return get_module_state(module);
}

CapnpModuleState* get_module_state(PyObject* module)
{ return static_cast<CapnpModuleState*>(PyModule_GetState(module)); }

void set_initializing_module(PyObject* module)
{ INITIALIZING_MODULE = module; }

int clear_module_state(PyObject* module)
{
    auto* state = get_module_state(module);
    if (!state) {
        return 0;
    }

    for (auto& item: state->type_registry) {
        item.second = {};
    }
    for (auto& item: state->enum_registry) {
        item.second = {};
    }

    state->enum_class              = {};
    state->enum_field_value_type   = {};
    state->capnp_struct_type       = {};
    state->capnp_union_struct_type = {};
    state->type_registry.clear();
    state->enum_registry.clear();
    return 0;
}

int traverse_module_state(PyObject* module, visitproc visit, void* arg)
{
    auto* state = get_module_state(module);
    if (!state) {
        return 0;
    }

    PyObject* enum_class              = state->enum_class.get();
    PyObject* enum_field_value_type   = state->enum_field_value_type.get();
    PyObject* capnp_struct_type       = state->capnp_struct_type.get();
    PyObject* capnp_union_struct_type = state->capnp_union_struct_type.get();
    Py_VISIT(enum_class);
    Py_VISIT(enum_field_value_type);
    Py_VISIT(capnp_struct_type);
    Py_VISIT(capnp_union_struct_type);

    for (const auto& item: state->type_registry) {
        PyObject* type_object = item.second.get();
        Py_VISIT(type_object);
    }
    for (const auto& item: state->enum_registry) {
        PyObject* enum_object = item.second.get();
        Py_VISIT(enum_object);
    }

    return 0;
}

}  // namespace scaler::protocol::pymod

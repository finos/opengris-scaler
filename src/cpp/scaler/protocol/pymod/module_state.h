#pragma once

#include <cstdint>
#include <unordered_map>

#include "scaler/protocol/pymod/schema_registry.h"
#include "scaler/utility/pymod/compatibility.h"

namespace scaler::protocol::pymod {

using scaler::utility::pymod::OwnedPyObject;

struct CapnpModuleState {
    SchemaRegistry schema_registry {};
    OwnedPyObject<> enum_class {};
    OwnedPyObject<> enum_field_value_type {};
    OwnedPyObject<> capnp_struct_type {};
    OwnedPyObject<> capnp_union_struct_type {};
    std::unordered_map<uint64_t, OwnedPyObject<>> type_registry;
    std::unordered_map<uint64_t, OwnedPyObject<>> enum_registry;
};

CapnpModuleState* get_module_state();
CapnpModuleState* get_module_state(PyObject* module);
void set_initializing_module(PyObject* module);
int clear_module_state(PyObject* module);
int traverse_module_state(PyObject* module, visitproc visit, void* arg);

}  // namespace scaler::protocol::pymod

#pragma once

#include <cstdint>

#include "scaler/utility/pymod/compatibility.h"

namespace scaler::protocol::pymod {

using scaler::utility::pymod::OwnedPyObject;

extern PyModuleDef MODULE_DEF;

OwnedPyObject<> get_type_by_schema_id(uint64_t schema_id);
OwnedPyObject<> get_enum_by_schema_id(uint64_t schema_id);
OwnedPyObject<> get_enum_field_value_type();
OwnedPyObject<> get_module_descriptor(const char* module_name);
bool initialize_runtime_modules(PyObject* module);

}  // namespace scaler::protocol::pymod

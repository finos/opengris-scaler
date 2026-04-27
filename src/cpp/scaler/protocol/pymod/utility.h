#pragma once

#include <capnp/dynamic.h>

#include <cstdint>

#include "scaler/utility/pymod/compatibility.h"

namespace scaler::protocol::pymod {

using scaler::utility::pymod::OwnedPyObject;

OwnedPyObject<> get_attr(PyObject* obj, const char* name);
bool read_enum_raw(PyObject* obj, uint16_t& out);
bool load_buffer(PyObject* obj, Py_buffer& buffer);
OwnedPyObject<> capnp_struct_init_method(PyObject* self, PyObject* args, PyObject* kwargs);
OwnedPyObject<> capnp_struct_get_attr(PyObject* self, PyObject* args);
OwnedPyObject<> capnp_struct_to_bytes(PyObject* self);
OwnedPyObject<> capnp_struct_from_bytes(PyObject* cls, PyObject* args, PyObject* kwargs);
OwnedPyObject<> capnp_union_init_method(PyObject* self, PyObject* args, PyObject* kwargs);
OwnedPyObject<> capnp_union_which(PyObject* self);
OwnedPyObject<> capnp_union_get_attr(PyObject* self, PyObject* args);
OwnedPyObject<> capnp_union_to_bytes(PyObject* self);
OwnedPyObject<> capnp_union_from_bytes(PyObject* cls, PyObject* args, PyObject* kwargs);
bool set_dynamic_field(capnp::DynamicStruct::Builder builder, capnp::StructSchema::Field field, PyObject* value);
bool set_dynamic_struct(capnp::DynamicStruct::Builder builder, PyObject* obj);
OwnedPyObject<> dynamic_value_to_py_object(
    capnp::DynamicValue::Reader value,
    capnp::Type type,
    PyObject* source,
    unsigned long long traversal_limit,
    uint64_t root_schema_id,
    PyObject* path);

}  // namespace scaler::protocol::pymod

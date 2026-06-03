#pragma once

#include <Python.h>

#include "scaler/utility/pymod/compatibility.h"

namespace scaler::protocol::pymod {

using scaler::utility::pymod::OwnedPyObject;

OwnedPyObject<> message_to_bytes(const char* variant_name, PyObject* inner);
OwnedPyObject<> message_from_bytes(PyObject* data, unsigned long long traversal_limit);
OwnedPyObject<> struct_to_bytes(const char* type_name, PyObject* obj);
OwnedPyObject<> struct_from_bytes(const char* type_name, PyObject* data, unsigned long long traversal_limit);

}  // namespace scaler::protocol::pymod

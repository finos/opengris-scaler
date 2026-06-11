#include "scaler/protocol/pymod/serialize.h"

#include <capnp/dynamic.h>
#include <capnp/message.h>
#include <capnp/serialize.h>
#include <kj/array.h>
#include <kj/exception.h>

#include <cstring>
#include <exception>
#include <stdexcept>

#include "protocol/message.capnp.h"
#include "scaler/protocol/pymod/module_state.h"
#include "scaler/protocol/pymod/schema_registry.h"
#include "scaler/protocol/pymod/utility.h"

namespace scaler::protocol::pymod {

// Wasm-relocator-safe string literals (see utility.cpp / bootstrap.cpp headers
// for the full rationale). Every bare ``"..."`` passed to the CPython C API
// must be stored as a named ``static const char[]`` array so the Pyodide
// SIDE_MODULE loader does not corrupt it via rodata section merging.
static const char ERR_MODULE_STATE[]    = "capnp module state is unavailable";
static const char ERR_NOT_ALIGNED[]     = "Cap'n Proto input buffer must be word-aligned for zero-copy reads";
static const char ERR_UNKNOWN_STRUCT[]  = "unknown Cap'n Proto struct type: %s";
static const char ERR_FROM_BYTES_DATA[] = "from_bytes requires data argument";
static const char ERR_NO_SUCH_VARIANT[] = "no such Message variant: %s";
static const char ERR_SERIALIZE[]       = "Cap'n Proto serialization failed";

namespace {

using scaler::utility::pymod::OwnedPyObject;

OwnedPyObject<> builder_to_bytes(capnp::MessageBuilder& builder)
{
    auto flat  = capnp::messageToFlatArray(builder);
    auto bytes = flat.asBytes();
    return OwnedPyObject<> {
        PyBytes_FromStringAndSize(reinterpret_cast<const char*>(bytes.begin()), static_cast<Py_ssize_t>(bytes.size()))};
}

OwnedPyObject<> read_struct_from_buffer(
    Py_buffer& buffer, PyObject* data, capnp::StructSchema schema, unsigned long long traversal_limit)
{
    capnp::ReaderOptions options;
    options.traversalLimitInWords = traversal_limit;
    auto words                    = kj::arrayPtr(
        reinterpret_cast<const capnp::word*>(buffer.buf), static_cast<size_t>(buffer.len) / sizeof(capnp::word));
    try {
        capnp::FlatArrayMessageReader reader(words, options);
        auto root = reader.getRoot<capnp::DynamicStruct>(schema);
        OwnedPyObject<> source {PyMemoryView_FromObject(data)};
        OwnedPyObject<> path {PyTuple_New(0)};
        if (!source || !path) {
            PyBuffer_Release(&buffer);
            return {};
        }
        OwnedPyObject<> result {dynamic_value_to_py_object(
            root, schema, source.get(), traversal_limit, schema.getProto().getId(), path.get())};
        PyBuffer_Release(&buffer);
        return result;
    } catch (const kj::Exception& e) {
        PyBuffer_Release(&buffer);
        if (!PyErr_Occurred()) {
            PyErr_SetString(PyExc_RuntimeError, e.getDescription().cStr());
        }
        return {};
    } catch (const std::exception& e) {
        PyBuffer_Release(&buffer);
        if (!PyErr_Occurred()) {
            PyErr_SetString(PyExc_RuntimeError, e.what());
        }
        return {};
    }
}

}  // namespace

OwnedPyObject<> message_to_bytes(const char* variant_name, PyObject* inner)
{
    auto* state = get_module_state();
    if (!state || !state->schema_registry.init()) {
        PyErr_SetString(PyExc_RuntimeError, ERR_MODULE_STATE);
        return {};
    }
    try {
        auto message_schema = capnp::Schema::from<scaler::protocol::Message>().asStruct();
        auto maybe_field    = message_schema.findFieldByName(variant_name);
        KJ_IF_MAYBE (field_ptr, maybe_field) {
            capnp::MallocMessageBuilder builder;
            auto root = builder.initRoot<capnp::DynamicStruct>(message_schema);
            if (!set_dynamic_field(root, *field_ptr, inner)) {
                return nullptr;
            }
            return builder_to_bytes(builder);
        } else {
            PyErr_Format(PyExc_KeyError, ERR_NO_SUCH_VARIANT, variant_name ? variant_name : "<null>");
            return {};
        }
    } catch (const kj::Exception&) {
        if (!PyErr_Occurred()) {
            PyErr_SetString(PyExc_RuntimeError, ERR_SERIALIZE);
        }
        return {};
    } catch (const std::exception&) {
        if (!PyErr_Occurred()) {
            PyErr_SetString(PyExc_RuntimeError, ERR_SERIALIZE);
        }
        return {};
    }
}

OwnedPyObject<> message_from_bytes(PyObject* data, unsigned long long traversal_limit)
{
    Py_buffer buffer {};
    if (PyObject_GetBuffer(data, &buffer, PyBUF_SIMPLE) < 0) {
        return {};
    }

    auto* state = get_module_state();
    if (!state || !state->schema_registry.init()) {
        PyBuffer_Release(&buffer);
        PyErr_SetString(PyExc_RuntimeError, ERR_MODULE_STATE);
        return {};
    }
    if (!check_word_alignment(buffer)) {
        PyBuffer_Release(&buffer);
        PyErr_SetString(PyExc_ValueError, ERR_NOT_ALIGNED);
        return {};
    }

    return read_struct_from_buffer(
        buffer, data, capnp::Schema::from<scaler::protocol::Message>().asStruct(), traversal_limit);
}

OwnedPyObject<> struct_to_bytes(const char* type_name, PyObject* obj)
{
    auto* state = get_module_state();
    if (!state || !state->schema_registry.init()) {
        PyErr_SetString(PyExc_RuntimeError, ERR_MODULE_STATE);
        return {};
    }
    capnp::StructSchema schema;
    try {
        schema = state->schema_registry.getStructByName(type_name);
    } catch (const std::out_of_range&) {
        PyErr_Format(PyExc_KeyError, ERR_UNKNOWN_STRUCT, type_name ? type_name : "<null>");
        return {};
    }

    try {
        capnp::MallocMessageBuilder builder;
        auto root = builder.initRoot<capnp::DynamicStruct>(schema);
        if (!set_dynamic_struct(root, obj)) {
            return nullptr;
        }
        return builder_to_bytes(builder);
    } catch (const kj::Exception&) {
        if (!PyErr_Occurred()) {
            PyErr_SetString(PyExc_RuntimeError, ERR_SERIALIZE);
        }
        return {};
    } catch (const std::exception&) {
        if (!PyErr_Occurred()) {
            PyErr_SetString(PyExc_RuntimeError, ERR_SERIALIZE);
        }
        return {};
    }
}

OwnedPyObject<> struct_from_bytes(const char* type_name, PyObject* data, unsigned long long traversal_limit)
{
    Py_buffer buffer {};
    if (PyObject_GetBuffer(data, &buffer, PyBUF_SIMPLE) < 0) {
        return {};
    }

    auto* state = get_module_state();
    if (!state || !state->schema_registry.init()) {
        PyBuffer_Release(&buffer);
        PyErr_SetString(PyExc_RuntimeError, ERR_MODULE_STATE);
        return {};
    }
    capnp::StructSchema schema;
    try {
        schema = state->schema_registry.getStructByName(type_name);
    } catch (const std::out_of_range&) {
        PyBuffer_Release(&buffer);
        PyErr_Format(PyExc_KeyError, ERR_UNKNOWN_STRUCT, type_name ? type_name : "<null>");
        return {};
    }

    if (!check_word_alignment(buffer)) {
        PyBuffer_Release(&buffer);
        PyErr_SetString(PyExc_ValueError, ERR_NOT_ALIGNED);
        return {};
    }

    return read_struct_from_buffer(buffer, data, schema, traversal_limit);
}

}  // namespace scaler::protocol::pymod

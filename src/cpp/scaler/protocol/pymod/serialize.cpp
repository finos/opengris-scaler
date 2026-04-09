#include "scaler/protocol/pymod/serialize.h"

#include <capnp/dynamic.h>
#include <capnp/message.h>
#include <capnp/serialize.h>
#include <kj/array.h>

#include <cstring>
#include <stdexcept>

#include "protocol/message.capnp.h"
#include "scaler/protocol/pymod/module_state.h"
#include "scaler/protocol/pymod/schema_registry.h"
#include "scaler/protocol/pymod/utility.h"

namespace scaler::protocol::pymod {

namespace {

using scaler::utility::pymod::OwnedPyObject;

OwnedPyObject<> builder_to_bytes(capnp::MessageBuilder& builder)
{
    auto flat  = capnp::messageToFlatArray(builder);
    auto bytes = flat.asBytes();
    return OwnedPyObject<> {
        PyBytes_FromStringAndSize(reinterpret_cast<const char*>(bytes.begin()), static_cast<Py_ssize_t>(bytes.size()))};
}

kj::Array<capnp::word> copy_bytes_to_words(const char* data, Py_ssize_t size)
{
    size_t word_count = (static_cast<size_t>(size) + sizeof(capnp::word) - 1) / sizeof(capnp::word);
    auto words        = kj::heapArray<capnp::word>(word_count);
    memset(words.begin(), 0, word_count * sizeof(capnp::word));
    memcpy(words.begin(), data, static_cast<size_t>(size));
    return words;
}

}  // namespace

OwnedPyObject<> message_to_bytes(const char* variant_name, PyObject* inner)
{
    auto* state = get_module_state();
    if (!state || !state->schema_registry.init()) {
        PyErr_SetString(PyExc_RuntimeError, "capnp module state is unavailable");
        return {};
    }
    auto message_schema = capnp::Schema::from<scaler::protocol::Message>().asStruct();
    capnp::MallocMessageBuilder builder;
    auto root  = builder.initRoot<capnp::DynamicStruct>(message_schema);
    auto field = message_schema.getFieldByName(variant_name);
    if (!set_dynamic_field(root, field, inner)) {
        return nullptr;
    }
    return builder_to_bytes(builder);
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
        PyErr_SetString(PyExc_RuntimeError, "capnp module state is unavailable");
        return {};
    }
    auto words = copy_bytes_to_words(static_cast<const char*>(buffer.buf), buffer.len);
    capnp::ReaderOptions options;
    options.traversalLimitInWords = traversal_limit;
    capnp::FlatArrayMessageReader reader(words.asPtr(), options);
    auto message_schema = capnp::Schema::from<scaler::protocol::Message>().asStruct();
    auto root           = reader.getRoot<capnp::DynamicStruct>(message_schema);
    OwnedPyObject<> result {dynamic_value_to_py_object(root, message_schema)};
    PyBuffer_Release(&buffer);
    return result;
}

OwnedPyObject<> struct_to_bytes(const char* type_name, PyObject* obj)
{
    auto* state = get_module_state();
    if (!state || !state->schema_registry.init()) {
        PyErr_SetString(PyExc_RuntimeError, "capnp module state is unavailable");
        return {};
    }
    capnp::StructSchema schema;
    try {
        schema = state->schema_registry.getStructByName(type_name);
    } catch (const std::out_of_range&) {
        PyErr_SetString(PyExc_KeyError, "unknown Cap'n Proto struct type");
        return {};
    }

    capnp::MallocMessageBuilder builder;
    auto root = builder.initRoot<capnp::DynamicStruct>(schema);
    if (!set_dynamic_struct(root, obj)) {
        return nullptr;
    }
    return builder_to_bytes(builder);
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
        PyErr_SetString(PyExc_RuntimeError, "capnp module state is unavailable");
        return {};
    }
    capnp::StructSchema schema;
    try {
        schema = state->schema_registry.getStructByName(type_name);
    } catch (const std::out_of_range&) {
        PyBuffer_Release(&buffer);
        PyErr_SetString(PyExc_KeyError, "unknown Cap'n Proto struct type");
        return {};
    }

    auto words = copy_bytes_to_words(static_cast<const char*>(buffer.buf), buffer.len);
    capnp::ReaderOptions options;
    options.traversalLimitInWords = traversal_limit;
    capnp::FlatArrayMessageReader reader(words.asPtr(), options);
    auto root = reader.getRoot<capnp::DynamicStruct>(schema);
    OwnedPyObject<> result {dynamic_value_to_py_object(root, schema)};
    PyBuffer_Release(&buffer);
    return result;
}

}  // namespace scaler::protocol::pymod

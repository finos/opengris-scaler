#include "scaler/protocol/pymod/utility.h"

#include <capnp/dynamic.h>

#include <type_traits>

#include "scaler/protocol/pymod/bootstrap.h"
#include "scaler/protocol/pymod/serialize.h"

namespace scaler::protocol::pymod {

using scaler::utility::pymod::OwnedPyObject;

OwnedPyObject<> get_attr(PyObject* obj, const char* name)
{
    return OwnedPyObject<> {PyObject_GetAttrString(obj, name)};
}

bool read_enum_raw(PyObject* obj, uint16_t& out)
{
    if (PyLong_Check(obj)) {
        out = static_cast<uint16_t>(PyLong_AsUnsignedLong(obj));
        return !PyErr_Occurred();
    }

    OwnedPyObject<> raw = get_attr(obj, "raw");
    if (raw) {
        out = static_cast<uint16_t>(PyLong_AsUnsignedLong(raw.get()));
        return !PyErr_Occurred();
    }
    PyErr_Clear();

    OwnedPyObject<> value = get_attr(obj, "value");
    if (!value) {
        return false;
    }

    out = static_cast<uint16_t>(PyLong_AsUnsignedLong(value.get()));
    return !PyErr_Occurred();
}

bool load_buffer(PyObject* obj, Py_buffer& buffer)
{
    if (PyObject_GetBuffer(obj, &buffer, PyBUF_SIMPLE) < 0) {
        return false;
    }
    return true;
}

namespace {

using DynamicListIndex = std::remove_cv_t<decltype(std::declval<capnp::DynamicList::Builder>().size())>;

PyObject* wrap_enum_value(PyObject* enum_type, PyObject* value)
{
    OwnedPyObject<> enum_field_value_type {get_enum_field_value_type()};
    if (!enum_field_value_type) {
        return nullptr;
    }
    if (PyObject_IsInstance(value, enum_field_value_type.get()) == 1) {
        return Py_NewRef(value);
    }
    if (PyObject_IsInstance(value, enum_type) == 1) {
        OwnedPyObject<> enum_value {get_attr(value, "value")};
        if (!enum_value) {
            return nullptr;
        }
        return PyObject_CallFunctionObjArgs(enum_field_value_type.get(), enum_value.get(), enum_type, nullptr);
    }
    if (PyLong_Check(value)) {
        return PyObject_CallFunctionObjArgs(enum_field_value_type.get(), value, enum_type, nullptr);
    }
    return Py_NewRef(value);
}

PyObject* wrap_enum_list_value(PyObject* enum_type, PyObject* values)
{
    OwnedPyObject<> enum_field_value_type {get_enum_field_value_type()};
    if (!enum_field_value_type) {
        return nullptr;
    }
    OwnedPyObject<> fast {PySequence_Fast(values, "expected a sequence")};
    if (!fast) {
        return nullptr;
    }
    Py_ssize_t size = PySequence_Fast_GET_SIZE(fast.get());
    OwnedPyObject<> result {PyList_New(size)};
    if (!result) {
        return nullptr;
    }
    for (Py_ssize_t index = 0; index < size; ++index) {
        PyObject* item = PySequence_Fast_GET_ITEM(fast.get(), index);
        OwnedPyObject<> wrapped;
        if (PyUnicode_Check(item)) {
            OwnedPyObject<> enum_member {PyObject_GetItem(enum_type, item)};
            if (!enum_member) {
                return nullptr;
            }
            OwnedPyObject<> enum_value {get_attr(enum_member.get(), "value")};
            if (!enum_value) {
                return nullptr;
            }
            wrapped = OwnedPyObject<> {
                PyObject_CallFunctionObjArgs(enum_field_value_type.get(), enum_value.get(), enum_type, nullptr)};
        } else {
            wrapped = OwnedPyObject<> {wrap_enum_value(enum_type, item)};
        }
        if (!wrapped) {
            return nullptr;
        }
        PyList_SetItem(result.get(), index, wrapped.take());
    }
    return result.take();
}

int capnp_struct_init(PyObject* self, PyObject* args, PyObject* kwargs)
{
    if (PyTuple_GET_SIZE(args) != 0) {
        PyErr_SetString(PyExc_TypeError, "expected keyword arguments only");
        return -1;
    }
    if (!kwargs) {
        return 0;
    }
    OwnedPyObject<> enum_fields {get_attr((PyObject*)Py_TYPE(self), "_enum_fields")};
    OwnedPyObject<> list_enum_fields {get_attr((PyObject*)Py_TYPE(self), "_list_enum_fields")};
    if (!enum_fields || !list_enum_fields) {
        return -1;
    }
    PyObject* key       = nullptr;
    PyObject* value     = nullptr;
    Py_ssize_t position = 0;
    while (PyDict_Next(kwargs, &position, &key, &value)) {
        PyObject* normalized_value = value;
        Py_INCREF(normalized_value);
        if (PyDict_Check(enum_fields.get())) {
            PyObject* enum_type = PyDict_GetItem(enum_fields.get(), key);
            if (enum_type) {
                Py_DECREF(normalized_value);
                normalized_value = wrap_enum_value(enum_type, value);
            }
        }
        if (normalized_value && PyDict_Check(list_enum_fields.get())) {
            PyObject* enum_type = PyDict_GetItem(list_enum_fields.get(), key);
            if (enum_type) {
                Py_DECREF(normalized_value);
                normalized_value = wrap_enum_list_value(enum_type, value);
            }
        }
        if (!normalized_value) {
            return -1;
        }
        int status = PyObject_SetAttr(self, key, normalized_value);
        Py_DECREF(normalized_value);
        if (status < 0) {
            return -1;
        }
    }
    return 0;
}

OwnedPyObject<> build_enum_field_value(uint64_t enum_schema_id, uint16_t raw)
{
    OwnedPyObject<> enum_type {get_enum_by_schema_id(enum_schema_id)};
    OwnedPyObject<> enum_field_value_type {get_enum_field_value_type()};
    if (!enum_type || !enum_field_value_type) {
        return {};
    }
    OwnedPyObject<> args {Py_BuildValue("(kO)", static_cast<unsigned long>(raw), enum_type.get())};
    if (!args) {
        return {};
    }
    return OwnedPyObject<> {PyObject_Call(enum_field_value_type.get(), args.get(), nullptr)};
}

bool set_text_field(capnp::DynamicStruct::Builder builder, capnp::StructSchema::Field field, PyObject* value)
{
    Py_ssize_t size  = 0;
    const char* text = PyUnicode_AsUTF8AndSize(value, &size);
    if (!text) {
        return false;
    }
    builder.set(field, capnp::DynamicValue::Reader(capnp::Text::Reader(text, static_cast<size_t>(size))));
    return true;
}

bool set_data_field(capnp::DynamicStruct::Builder builder, capnp::StructSchema::Field field, PyObject* value)
{
    Py_buffer buffer;
    if (!load_buffer(value, buffer)) {
        return false;
    }
    capnp::Data::Reader data(reinterpret_cast<const kj::byte*>(buffer.buf), static_cast<size_t>(buffer.len));
    builder.set(field, capnp::DynamicValue::Reader(data));
    PyBuffer_Release(&buffer);
    return true;
}

bool set_dynamic_struct_impl(capnp::DynamicStruct::Builder builder, PyObject* obj);
bool set_dynamic_list_element(
    capnp::DynamicList::Builder builder, DynamicListIndex index, capnp::Type element_type, PyObject* value);

bool set_dynamic_list(capnp::DynamicList::Builder builder, capnp::ListSchema schema, PyObject* sequence)
{
    OwnedPyObject<> fast {PySequence_Fast(sequence, "expected a sequence")};
    if (!fast) {
        return false;
    }
    auto element_type = schema.getElementType();
    auto size         = static_cast<Py_ssize_t>(builder.size());
    for (Py_ssize_t index = 0; index < size; ++index) {
        PyObject* item = PySequence_Fast_GET_ITEM(fast.get(), index);
        if (!set_dynamic_list_element(builder, static_cast<DynamicListIndex>(index), element_type, item)) {
            return false;
        }
    }
    return true;
}

bool set_dynamic_list_element(
    capnp::DynamicList::Builder builder, DynamicListIndex index, capnp::Type element_type, PyObject* value)
{
    switch (element_type.which()) {
        case capnp::schema::Type::VOID: builder.set(index, capnp::DynamicValue::Reader(capnp::VOID)); return true;
        case capnp::schema::Type::BOOL:
            builder.set(index, capnp::DynamicValue::Reader(PyObject_IsTrue(value) == 1));
            return !PyErr_Occurred();
        case capnp::schema::Type::INT8:
        case capnp::schema::Type::INT16:
        case capnp::schema::Type::INT32:
        case capnp::schema::Type::INT64:
            builder.set(index, capnp::DynamicValue::Reader(PyLong_AsLongLong(value)));
            return !PyErr_Occurred();
        case capnp::schema::Type::UINT8:
        case capnp::schema::Type::UINT16:
        case capnp::schema::Type::UINT32:
        case capnp::schema::Type::UINT64:
            builder.set(index, capnp::DynamicValue::Reader(PyLong_AsUnsignedLongLong(value)));
            return !PyErr_Occurred();
        case capnp::schema::Type::FLOAT32:
        case capnp::schema::Type::FLOAT64:
            builder.set(index, capnp::DynamicValue::Reader(PyFloat_AsDouble(value)));
            return !PyErr_Occurred();
        case capnp::schema::Type::TEXT: {
            Py_ssize_t size  = 0;
            const char* text = PyUnicode_AsUTF8AndSize(value, &size);
            if (!text)
                return false;
            builder.set(index, capnp::DynamicValue::Reader(capnp::Text::Reader(text, static_cast<size_t>(size))));
            return true;
        }
        case capnp::schema::Type::DATA: {
            Py_buffer buffer;
            if (!load_buffer(value, buffer))
                return false;
            capnp::Data::Reader data(reinterpret_cast<const kj::byte*>(buffer.buf), static_cast<size_t>(buffer.len));
            builder.set(index, capnp::DynamicValue::Reader(data));
            PyBuffer_Release(&buffer);
            return true;
        }
        case capnp::schema::Type::ENUM: {
            uint16_t raw = 0;
            if (!read_enum_raw(value, raw))
                return false;
            builder.set(
                index, capnp::DynamicValue::Reader(capnp::DynamicEnum(element_type.asEnum().getEnumerants()[raw])));
            return true;
        }
        case capnp::schema::Type::STRUCT: {
            auto child = builder[index].as<capnp::DynamicStruct>();
            return set_dynamic_struct_impl(child, value);
        }
        case capnp::schema::Type::LIST: {
            OwnedPyObject<> fast {PySequence_Fast(value, "expected a sequence")};
            if (!fast)
                return false;
            auto child = builder.init(index, static_cast<DynamicListIndex>(PySequence_Fast_GET_SIZE(fast.get())))
                             .as<capnp::DynamicList>();
            return set_dynamic_list(child, element_type.asList(), fast.get());
        }
        default: PyErr_SetString(PyExc_TypeError, "unsupported Cap'n Proto list element type"); return false;
    }
}

bool set_dynamic_struct_impl(capnp::DynamicStruct::Builder builder, PyObject* obj)
{
    auto schema = builder.getSchema();
    for (auto field: schema.getFields()) {
        const char* field_name = field.getProto().getName().cStr();
        int has_attr           = PyObject_HasAttrString(obj, field_name);
        if (has_attr < 0)
            return false;
        if (has_attr == 0)
            continue;
        OwnedPyObject<> value {get_attr(obj, field_name)};
        if (!value || !set_dynamic_field(builder, field, value.get()))
            return false;
    }
    return true;
}

}  // namespace

OwnedPyObject<> capnp_struct_init_method(PyObject* self, PyObject* args, PyObject* kwargs)
{
    if (capnp_struct_init(self, args, kwargs) < 0)
        return {};
    return OwnedPyObject<>::none();
}

OwnedPyObject<> capnp_struct_to_bytes(PyObject* self)
{ return struct_to_bytes(Py_TYPE(self)->tp_name, self); }

OwnedPyObject<> capnp_struct_from_bytes(PyObject* cls, PyObject* args, PyObject* kwargs)
{
    PyObject* data                     = nullptr;
    unsigned long long traversal_limit = 2ULL * 9223372036854775807ULL + 1ULL;
    PyObject* effective_class          = cls;
    if (PyTuple_GET_SIZE(args) >= 1 && PyType_Check(PyTuple_GetItem(args, 0))) {
        effective_class = PyTuple_GetItem(args, 0);
        if (PyTuple_GET_SIZE(args) < 2) {
            PyErr_SetString(PyExc_TypeError, "from_bytes requires data argument");
            return {};
        }
        data = PyTuple_GetItem(args, 1);
        if (PyTuple_GET_SIZE(args) >= 3) {
            traversal_limit = PyLong_AsUnsignedLongLong(PyTuple_GetItem(args, 2));
            if (PyErr_Occurred())
                return {};
        }
    } else {
        static const char* keywords[] = {"data", "traversal_limit_in_words", nullptr};
        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|K", const_cast<char**>(keywords), &data, &traversal_limit)) {
            return {};
        }
    }
    return struct_from_bytes(((PyTypeObject*)effective_class)->tp_name, data, traversal_limit);
}

OwnedPyObject<> capnp_union_which(PyObject* self)
{ return PyObject_GetAttrString(self, "_variant_name"); }

OwnedPyObject<> capnp_union_get_attr(PyObject* self, PyObject* args)
{
    const char* name = nullptr;
    if (!PyArg_ParseTuple(args, "s", &name))
        return {};
    OwnedPyObject<> variant_name {get_attr(self, "_variant_name")};
    if (!variant_name)
        return {};
    int matches = PyUnicode_CompareWithASCIIString(variant_name.get(), name);
    if (matches == 0)
        return OwnedPyObject<> {PyObject_GenericGetAttr(self, PyUnicode_FromString(name))};
    PyErr_SetString(PyExc_AttributeError, name);
    return {};
}

OwnedPyObject<> capnp_union_to_bytes(PyObject* self)
{
    OwnedPyObject<> variant_name {get_attr(self, "_variant_name")};
    if (!variant_name)
        return {};
    OwnedPyObject<> payload {PyObject_GetAttr(self, variant_name.get())};
    if (!payload)
        return {};
    return message_to_bytes(PyUnicode_AsUTF8(variant_name.get()), payload.get());
}

OwnedPyObject<> capnp_union_from_bytes(PyObject* /*cls*/, PyObject* args, PyObject* kwargs)
{
    PyObject* data                     = nullptr;
    unsigned long long traversal_limit = 2ULL * 9223372036854775807ULL + 1ULL;
    if (PyTuple_GET_SIZE(args) >= 1 && PyType_Check(PyTuple_GetItem(args, 0))) {
        if (PyTuple_GET_SIZE(args) < 2) {
            PyErr_SetString(PyExc_TypeError, "from_bytes requires data argument");
            return {};
        }
        data = PyTuple_GetItem(args, 1);
        if (PyTuple_GET_SIZE(args) >= 3) {
            traversal_limit = PyLong_AsUnsignedLongLong(PyTuple_GetItem(args, 2));
            if (PyErr_Occurred())
                return {};
        }
    } else {
        static const char* keywords[] = {"data", "traversal_limit_in_words", nullptr};
        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|K", const_cast<char**>(keywords), &data, &traversal_limit)) {
            return {};
        }
    }
    return message_from_bytes(data, traversal_limit);
}

OwnedPyObject<> capnp_union_init_method(PyObject* self, PyObject* args, PyObject* kwargs)
{
    if (PyTuple_GET_SIZE(args) != 0) {
        PyErr_SetString(PyExc_TypeError, "expected keyword arguments only");
        return {};
    }
    if (!kwargs) {
        PyErr_SetString(PyExc_ValueError, "requires exactly one active union field");
        return {};
    }
    OwnedPyObject<> union_fields {get_attr((PyObject*)Py_TYPE(self), "_union_fields")};
    if (!union_fields)
        return {};
    PyObject* key            = nullptr;
    PyObject* value          = nullptr;
    Py_ssize_t position      = 0;
    const char* active_field = nullptr;
    int active_count         = 0;
    while (PyDict_Next(kwargs, &position, &key, &value)) {
        int contains = PySet_Contains(union_fields.get(), key);
        if (contains < 0)
            return {};
        if (contains == 1) {
            ++active_count;
            active_field = PyUnicode_AsUTF8(key);
        }
    }
    if (active_count != 1 || !active_field) {
        PyErr_SetString(PyExc_ValueError, "requires exactly one active union field");
        return {};
    }
    if (PyObject_SetAttrString(self, "_variant_name", PyUnicode_FromString(active_field)) < 0)
        return {};
    if (capnp_struct_init(self, args, kwargs) < 0)
        return {};
    return OwnedPyObject<>::none();
}

bool set_dynamic_field(capnp::DynamicStruct::Builder builder, capnp::StructSchema::Field field, PyObject* value)
{
    auto type = field.getType();
    switch (type.which()) {
        case capnp::schema::Type::VOID: builder.set(field, capnp::DynamicValue::Reader(capnp::VOID)); return true;
        case capnp::schema::Type::BOOL:
            builder.set(field, capnp::DynamicValue::Reader(PyObject_IsTrue(value) == 1));
            return !PyErr_Occurred();
        case capnp::schema::Type::INT8:
        case capnp::schema::Type::INT16:
        case capnp::schema::Type::INT32:
        case capnp::schema::Type::INT64:
            builder.set(field, capnp::DynamicValue::Reader(PyLong_AsLongLong(value)));
            return !PyErr_Occurred();
        case capnp::schema::Type::UINT8:
        case capnp::schema::Type::UINT16:
        case capnp::schema::Type::UINT32:
        case capnp::schema::Type::UINT64:
            builder.set(field, capnp::DynamicValue::Reader(PyLong_AsUnsignedLongLong(value)));
            return !PyErr_Occurred();
        case capnp::schema::Type::FLOAT32:
        case capnp::schema::Type::FLOAT64:
            builder.set(field, capnp::DynamicValue::Reader(PyFloat_AsDouble(value)));
            return !PyErr_Occurred();
        case capnp::schema::Type::TEXT: return set_text_field(builder, field, value);
        case capnp::schema::Type::DATA: return set_data_field(builder, field, value);
        case capnp::schema::Type::ENUM: {
            uint16_t raw = 0;
            if (!read_enum_raw(value, raw))
                return false;
            builder.set(field, capnp::DynamicValue::Reader(capnp::DynamicEnum(type.asEnum().getEnumerants()[raw])));
            return true;
        }
        case capnp::schema::Type::STRUCT: {
            auto child = builder.init(field).as<capnp::DynamicStruct>();
            return set_dynamic_struct_impl(child, value);
        }
        case capnp::schema::Type::LIST: {
            OwnedPyObject<> fast {PySequence_Fast(value, "expected a sequence")};
            if (!fast)
                return false;
            auto child = builder.init(field, static_cast<DynamicListIndex>(PySequence_Fast_GET_SIZE(fast.get())))
                             .as<capnp::DynamicList>();
            return set_dynamic_list(child, type.asList(), fast.get());
        }
        default: PyErr_SetString(PyExc_TypeError, "unsupported Cap'n Proto field type"); return false;
    }
}

bool set_dynamic_struct(capnp::DynamicStruct::Builder builder, PyObject* obj)
{ return set_dynamic_struct_impl(builder, obj); }

OwnedPyObject<> dynamic_value_to_py_object(capnp::DynamicValue::Reader value, capnp::Type type)
{
    switch (type.which()) {
        case capnp::schema::Type::VOID: return OwnedPyObject<>::none();
        case capnp::schema::Type::BOOL: return OwnedPyObject<> {PyBool_FromLong(value.as<bool>() ? 1 : 0)};
        case capnp::schema::Type::INT8:
        case capnp::schema::Type::INT16:
        case capnp::schema::Type::INT32:
        case capnp::schema::Type::INT64: return OwnedPyObject<> {PyLong_FromLongLong(value.as<int64_t>())};
        case capnp::schema::Type::UINT8:
        case capnp::schema::Type::UINT16:
        case capnp::schema::Type::UINT32:
        case capnp::schema::Type::UINT64: return OwnedPyObject<> {PyLong_FromUnsignedLongLong(value.as<uint64_t>())};
        case capnp::schema::Type::FLOAT32:
        case capnp::schema::Type::FLOAT64: return OwnedPyObject<> {PyFloat_FromDouble(value.as<double>())};
        case capnp::schema::Type::TEXT: {
            auto text = value.as<capnp::Text>();
            return OwnedPyObject<> {PyUnicode_FromStringAndSize(text.begin(), static_cast<Py_ssize_t>(text.size()))};
        }
        case capnp::schema::Type::DATA: {
            auto data = value.as<capnp::Data>();
            return OwnedPyObject<> {PyBytes_FromStringAndSize(
                reinterpret_cast<const char*>(data.begin()), static_cast<Py_ssize_t>(data.size()))};
        }
        case capnp::schema::Type::ENUM: {
            auto dynamic_enum = value.as<capnp::DynamicEnum>();
            return build_enum_field_value(type.asEnum().getProto().getId(), dynamic_enum.getRaw());
        }
        case capnp::schema::Type::STRUCT: {
            OwnedPyObject<> type_object {get_type_by_schema_id(type.asStruct().getProto().getId())};
            if (!type_object)
                return nullptr;
            OwnedPyObject<> kwargs {PyDict_New()};
            if (!kwargs)
                return nullptr;
            auto reader_struct      = value.as<capnp::DynamicStruct>();
            auto active_union_field = reader_struct.which();
            for (auto field: type.asStruct().getFields()) {
                bool is_union_field = field.getProto().getDiscriminantValue() != capnp::schema::Field::NO_DISCRIMINANT;
                if (is_union_field) {
                    KJ_IF_MAYBE (active_union_field_ptr, active_union_field) {
                        if (field != *active_union_field_ptr)
                            continue;
                    } else {
                        continue;
                    }
                }
                OwnedPyObject<> item {dynamic_value_to_py_object(reader_struct.get(field), field.getType())};
                if (!item || PyDict_SetItemString(kwargs.get(), field.getProto().getName().cStr(), item.get()) < 0)
                    return nullptr;
            }
            OwnedPyObject<> empty_args {PyTuple_New(0)};
            if (!empty_args)
                return {};
            return OwnedPyObject<> {PyObject_Call(type_object.get(), empty_args.get(), kwargs.get())};
        }
        case capnp::schema::Type::LIST: {
            auto list_reader = value.as<capnp::DynamicList>();
            OwnedPyObject<> list {PyList_New(list_reader.size())};
            if (!list)
                return nullptr;
            auto element_type = type.asList().getElementType();
            for (DynamicListIndex index = 0; index < list_reader.size(); ++index) {
                OwnedPyObject<> item {dynamic_value_to_py_object(list_reader[index], element_type)};
                if (!item)
                    return nullptr;
                PyList_SetItem(list.get(), index, item.take());
            }
            return list;
        }
        default: PyErr_SetString(PyExc_TypeError, "unsupported Cap'n Proto dynamic value type"); return {};
    }
}

}  // namespace scaler::protocol::pymod

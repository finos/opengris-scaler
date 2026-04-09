#include "scaler/protocol/pymod/bootstrap.h"

#include <capnp/dynamic.h>
#include <capnp/schema.h>

#include <string>
#include <vector>

#include "scaler/protocol/pymod/module_state.h"
#include "scaler/protocol/pymod/schema_registry.h"
#include "scaler/protocol/pymod/utility.h"

using scaler::utility::pymod::OwnedPyObject;

namespace scaler::protocol::pymod {

namespace {

OwnedPyObject<> build_schema_descriptor(capnp::Schema schema)
{
    auto proto = schema.getProto();

    OwnedPyObject<> descriptor {PyDict_New()};
    if (!descriptor) {
        return nullptr;
    }

    const char* kind = proto.isEnum() ? "enum" : "struct";
    PyDict_SetItemString(descriptor.get(), "kind", OwnedPyObject<>(PyUnicode_FromString(kind)).get());
    PyDict_SetItemString(
        descriptor.get(), "name", OwnedPyObject<>(PyUnicode_FromString(schema.getUnqualifiedName().cStr())).get());
    PyDict_SetItemString(descriptor.get(), "id", OwnedPyObject<>(PyLong_FromUnsignedLongLong(proto.getId())).get());

    if (proto.isEnum()) {
        auto enum_schema = schema.asEnum();
        OwnedPyObject<> members {PyList_New(0)};
        if (!members) {
            return nullptr;
        }

        for (auto enumerant: enum_schema.getEnumerants()) {
            OwnedPyObject<> tuple {
                Py_BuildValue("(sk)", enumerant.getProto().getName().cStr(), (unsigned long)enumerant.getOrdinal())};
            if (!tuple || PyList_Append(members.get(), tuple.get()) < 0) {
                return nullptr;
            }
        }

        PyDict_SetItemString(descriptor.get(), "members", members.get());
        return descriptor;
    }

    auto struct_schema = schema.asStruct();
    OwnedPyObject<> enum_fields {PyDict_New()};
    OwnedPyObject<> list_enum_fields {PyDict_New()};
    OwnedPyObject<> union_fields {PyList_New(0)};
    OwnedPyObject<> children {PyList_New(0)};
    if (!enum_fields || !list_enum_fields || !union_fields || !children) {
        return nullptr;
    }

    for (auto field: struct_schema.getFields()) {
        auto field_type        = field.getType();
        const char* field_name = field.getProto().getName().cStr();

        if (field.getProto().getDiscriminantValue() != capnp::schema::Field::NO_DISCRIMINANT) {
            if (PyList_Append(union_fields.get(), OwnedPyObject<>(PyUnicode_FromString(field_name)).get()) < 0) {
                return nullptr;
            }
        }

        if (field_type.isEnum()) {
            if (PyDict_SetItemString(
                    enum_fields.get(),
                    field_name,
                    OwnedPyObject<>(PyLong_FromUnsignedLongLong(field_type.asEnum().getProto().getId())).get()) < 0) {
                return nullptr;
            }
        } else if (field_type.isList() && field_type.asList().getElementType().isEnum()) {
            if (PyDict_SetItemString(
                    list_enum_fields.get(),
                    field_name,
                    OwnedPyObject<>(
                        PyLong_FromUnsignedLongLong(field_type.asList().getElementType().asEnum().getProto().getId()))
                        .get()) < 0) {
                return nullptr;
            }
        }
    }

    auto nested_nodes = proto.getNestedNodes();
    for (decltype(nested_nodes.size()) index = 0; index < nested_nodes.size(); ++index) {
        auto* state = get_module_state();
        if (!state) {
            PyErr_SetString(PyExc_RuntimeError, "capnp module state is unavailable");
            return nullptr;
        }
        auto nested_schema = state->schema_registry.getSchemaById(nested_nodes[index].getId());
        OwnedPyObject<> child_descriptor {build_schema_descriptor(nested_schema)};
        if (!child_descriptor || PyList_Append(children.get(), child_descriptor.get()) < 0) {
            return nullptr;
        }
    }

    PyDict_SetItemString(descriptor.get(), "enum_fields", enum_fields.get());
    PyDict_SetItemString(descriptor.get(), "list_enum_fields", list_enum_fields.get());
    PyDict_SetItemString(descriptor.get(), "union_fields", union_fields.get());
    PyDict_SetItemString(descriptor.get(), "children", children.get());
    return descriptor;
}

OwnedPyObject<> make_builtin_function(const PyMethodDef* def)
{ return OwnedPyObject<> {PyCFunction_NewEx(const_cast<PyMethodDef*>(def), nullptr, nullptr)}; }

OwnedPyObject<> make_class_method(const PyMethodDef* def)
{
    OwnedPyObject<> function {make_builtin_function(def)};
    if (!function) {
        return {};
    }
    return OwnedPyObject<> {PyClassMethod_New(function.get())};
}

OwnedPyObject<> make_method_descriptor(PyObject* type, const PyMethodDef* def)
{ return OwnedPyObject<> {PyDescr_NewMethod((PyTypeObject*)type, const_cast<PyMethodDef*>(def))}; }

bool register_module(PyObject* module, const char* full_name)
{ return PyDict_SetItemString(PyImport_GetModuleDict(), full_name, module) == 0; }

OwnedPyObject<> create_python_class(const char* name, PyObject* bases, PyObject* dict)
{
    OwnedPyObject<> name_object {PyUnicode_FromString(name)};
    if (!name_object) {
        return {};
    }
    return OwnedPyObject<> {
        PyObject_CallFunctionObjArgs((PyObject*)&PyType_Type, name_object.get(), bases, dict, nullptr)};
}

PyObject* py_capnp_struct_init_method(PyObject* self, PyObject* args, PyObject* kwargs)
{ return ::scaler::protocol::pymod::capnp_struct_init_method(self, args, kwargs).take(); }

PyObject* py_capnp_struct_to_bytes(PyObject* self, PyObject* /*unused*/)
{ return ::scaler::protocol::pymod::capnp_struct_to_bytes(self).take(); }

PyObject* py_capnp_struct_from_bytes(PyObject* cls, PyObject* args, PyObject* kwargs)
{ return ::scaler::protocol::pymod::capnp_struct_from_bytes(cls, args, kwargs).take(); }

PyObject* py_capnp_union_init_method(PyObject* self, PyObject* args, PyObject* kwargs)
{ return ::scaler::protocol::pymod::capnp_union_init_method(self, args, kwargs).take(); }

PyObject* py_capnp_union_which(PyObject* self, PyObject* /*unused*/)
{ return ::scaler::protocol::pymod::capnp_union_which(self).take(); }

PyObject* py_capnp_union_get_attr(PyObject* self, PyObject* args)
{ return ::scaler::protocol::pymod::capnp_union_get_attr(self, args).take(); }

PyObject* py_capnp_union_to_bytes(PyObject* self, PyObject* /*unused*/)
{ return ::scaler::protocol::pymod::capnp_union_to_bytes(self).take(); }

PyObject* py_capnp_union_from_bytes(PyObject* cls, PyObject* args, PyObject* kwargs)
{ return ::scaler::protocol::pymod::capnp_union_from_bytes(cls, args, kwargs).take(); }

static PyObject* enum_field_value_init(PyObject* self, PyObject* args)
{
    PyObject* raw       = nullptr;
    PyObject* enum_type = nullptr;
    if (!PyArg_ParseTuple(args, "OO", &raw, &enum_type)) {
        return nullptr;
    }
    if (PyObject_SetAttrString(self, "raw", raw) < 0 || PyObject_SetAttrString(self, "_enum_type", enum_type) < 0) {
        return nullptr;
    }
    OwnedPyObject<> member {PyObject_CallOneArg(enum_type, raw)};
    if (!member) {
        if (!PyErr_ExceptionMatches(PyExc_ValueError)) {
            return nullptr;
        }
        PyErr_Clear();
        member = OwnedPyObject<>::none();
    }
    if (PyObject_SetAttrString(self, "_member", member.get()) < 0) {
        return nullptr;
    }
    Py_RETURN_NONE;
}

static PyObject* enum_field_value_as_str(PyObject* self, PyObject* /*unused*/)
{
    OwnedPyObject<> member {get_attr(self, "_member")};
    if (!member) {
        return nullptr;
    }
    if (member.is_none()) {
        OwnedPyObject<> raw {get_attr(self, "raw")};
        if (!raw) {
            return nullptr;
        }
        return PyObject_Str(raw.get());
    }
    return PyObject_GetAttrString(member.get(), "name");
}

static PyObject* enum_field_value_eq(PyObject* self, PyObject* other)
{
    OwnedPyObject<> raw {get_attr(self, "raw")};
    if (!raw) {
        return nullptr;
    }
    if (PyLong_Check(other)) {
        return PyBool_FromLong(PyObject_RichCompareBool(raw.get(), other, Py_EQ));
    }
    auto* state = get_module_state();
    if (state && state->enum_class && PyObject_IsInstance(other, state->enum_class.get()) == 1) {
        OwnedPyObject<> other_value {get_attr(other, "value")};
        if (!other_value) {
            return nullptr;
        }
        return PyBool_FromLong(PyObject_RichCompareBool(raw.get(), other_value.get(), Py_EQ));
    }
    if (state && state->enum_field_value_type && PyObject_IsInstance(other, state->enum_field_value_type.get()) == 1) {
        OwnedPyObject<> other_raw {get_attr(other, "raw")};
        if (!other_raw) {
            return nullptr;
        }
        return PyBool_FromLong(PyObject_RichCompareBool(raw.get(), other_raw.get(), Py_EQ));
    }
    Py_RETURN_NOTIMPLEMENTED;
}

static PyObject* enum_field_value_hash(PyObject* self, PyObject* /*unused*/)
{
    OwnedPyObject<> raw {get_attr(self, "raw")};
    if (!raw) {
        return nullptr;
    }
    auto hash = PyObject_Hash(raw.get());
    if (hash == -1 && PyErr_Occurred()) {
        return nullptr;
    }
    return PyLong_FromSsize_t(hash);
}

static PyObject* enum_field_value_int(PyObject* self, PyObject* /*unused*/)
{ return PyObject_GetAttrString(self, "raw"); }

static PyMethodDef ENUM_FIELD_VALUE_INIT_DEF = {"__init__", (PyCFunction)enum_field_value_init, METH_VARARGS, nullptr};
static PyMethodDef ENUM_FIELD_VALUE_AS_STR_DEF = {
    "_as_str", (PyCFunction)enum_field_value_as_str, METH_NOARGS, nullptr};
static PyMethodDef ENUM_FIELD_VALUE_EQ_DEF   = {"__eq__", (PyCFunction)enum_field_value_eq, METH_O, nullptr};
static PyMethodDef ENUM_FIELD_VALUE_HASH_DEF = {"__hash__", (PyCFunction)enum_field_value_hash, METH_NOARGS, nullptr};
static PyMethodDef ENUM_FIELD_VALUE_INT_DEF  = {"__int__", (PyCFunction)enum_field_value_int, METH_NOARGS, nullptr};
static PyMethodDef CAPNP_STRUCT_INIT_DEF     = {
    "__init__", (PyCFunction)(void (*)(void))py_capnp_struct_init_method, METH_VARARGS | METH_KEYWORDS, nullptr};
static PyMethodDef CAPNP_STRUCT_TO_BYTES_DEF = {
    "to_bytes", (PyCFunction)py_capnp_struct_to_bytes, METH_NOARGS, nullptr};
static PyMethodDef CAPNP_STRUCT_FROM_BYTES_DEF = {
    "from_bytes", (PyCFunction)(void (*)(void))py_capnp_struct_from_bytes, METH_VARARGS | METH_KEYWORDS, nullptr};
static PyMethodDef CAPNP_UNION_INIT_DEF = {
    "__init__", (PyCFunction)(void (*)(void))py_capnp_union_init_method, METH_VARARGS | METH_KEYWORDS, nullptr};
static PyMethodDef CAPNP_UNION_WHICH_DEF   = {"which", (PyCFunction)py_capnp_union_which, METH_NOARGS, nullptr};
static PyMethodDef CAPNP_UNION_GETATTR_DEF = {
    "__getattr__", (PyCFunction)py_capnp_union_get_attr, METH_VARARGS, nullptr};
static PyMethodDef CAPNP_UNION_TO_BYTES_DEF = {"to_bytes", (PyCFunction)py_capnp_union_to_bytes, METH_NOARGS, nullptr};
static PyMethodDef CAPNP_UNION_FROM_BYTES_DEF = {
    "from_bytes", (PyCFunction)(void (*)(void))py_capnp_union_from_bytes, METH_VARARGS | METH_KEYWORDS, nullptr};

OwnedPyObject<> create_enum_type(PyObject* descriptor, const char* module_name)
{
    OwnedPyObject<> name {Py_NewRef(PyDict_GetItemString(descriptor, "name"))};
    OwnedPyObject<> members_list {Py_NewRef(PyDict_GetItemString(descriptor, "members"))};
    OwnedPyObject<> schema_id_obj {Py_NewRef(PyDict_GetItemString(descriptor, "id"))};
    if (!name || !members_list || !schema_id_obj) {
        return nullptr;
    }
    OwnedPyObject<> members_dict {PyDict_New()};
    if (!members_dict) {
        return nullptr;
    }
    Py_ssize_t size = PyList_Size(members_list.get());
    for (Py_ssize_t index = 0; index < size; ++index) {
        PyObject* item = PyList_GetItem(members_list.get(), index);
        if (PyDict_SetItem(members_dict.get(), PyTuple_GetItem(item, 0), PyTuple_GetItem(item, 1)) < 0) {
            return nullptr;
        }
    }
    auto* state = get_module_state();
    if (!state || !state->enum_class) {
        PyErr_SetString(PyExc_RuntimeError, "capnp enum class state is unavailable");
        return {};
    }
    OwnedPyObject<> enum_type {
        PyObject_CallFunctionObjArgs(state->enum_class.get(), name.get(), members_dict.get(), nullptr)};
    if (!enum_type) {
        return nullptr;
    }
    OwnedPyObject<> module_name_obj {PyUnicode_FromString(module_name)};
    if (!module_name_obj || PyObject_SetAttrString(enum_type.get(), "__module__", module_name_obj.get()) < 0 ||
        PyObject_SetAttrString(enum_type.get(), "_schema_node_id", schema_id_obj.get()) < 0) {
        return nullptr;
    }
    state->enum_registry[PyLong_AsUnsignedLongLong(schema_id_obj.get())] =
        OwnedPyObject<>::fromBorrowed(enum_type.get());
    return enum_type;
}

OwnedPyObject<> build_node_from_descriptor(
    PyObject* descriptor, const char* module_name, std::vector<std::pair<OwnedPyObject<>, OwnedPyObject<>>>& pending);

OwnedPyObject<> create_struct_type(
    PyObject* descriptor, const char* module_name, std::vector<std::pair<OwnedPyObject<>, OwnedPyObject<>>>& pending)
{
    OwnedPyObject<> name {Py_NewRef(PyDict_GetItemString(descriptor, "name"))};
    OwnedPyObject<> schema_id_obj {Py_NewRef(PyDict_GetItemString(descriptor, "id"))};
    OwnedPyObject<> union_fields {Py_NewRef(PyDict_GetItemString(descriptor, "union_fields"))};
    OwnedPyObject<> children {Py_NewRef(PyDict_GetItemString(descriptor, "children"))};
    if (!name || !schema_id_obj || !union_fields || !children) {
        return nullptr;
    }
    auto* state = get_module_state();
    if (!state || !state->capnp_struct_type || !state->capnp_union_struct_type) {
        PyErr_SetString(PyExc_RuntimeError, "capnp base type state is unavailable");
        return nullptr;
    }
    OwnedPyObject<> bases {PyTuple_Pack(
        1,
        PyList_Size(union_fields.get()) > 0 ? state->capnp_union_struct_type.get() : state->capnp_struct_type.get())};
    OwnedPyObject<> dict {PyDict_New()};
    OwnedPyObject<> module_name_obj {PyUnicode_FromString(module_name)};
    OwnedPyObject<> enum_fields {PyDict_New()};
    OwnedPyObject<> list_enum_fields {PyDict_New()};
    OwnedPyObject<> union_field_set {PySet_New(union_fields.get())};
    if (!bases || !dict || !module_name_obj || !enum_fields || !list_enum_fields || !union_field_set) {
        return nullptr;
    }
    PyDict_SetItemString(dict.get(), "__module__", module_name_obj.get());
    PyDict_SetItemString(dict.get(), "_schema_node_id", schema_id_obj.get());
    PyDict_SetItemString(dict.get(), "_enum_fields", enum_fields.get());
    PyDict_SetItemString(dict.get(), "_list_enum_fields", list_enum_fields.get());
    PyDict_SetItemString(dict.get(), "_union_fields", union_field_set.get());
    OwnedPyObject<> type {create_python_class(PyUnicode_AsUTF8(name.get()), bases.get(), dict.get())};
    if (!type) {
        return nullptr;
    }
    state->type_registry[PyLong_AsUnsignedLongLong(schema_id_obj.get())] = OwnedPyObject<>::fromBorrowed(type.get());
    Py_ssize_t child_count                                               = PyList_Size(children.get());
    for (Py_ssize_t index = 0; index < child_count; ++index) {
        PyObject* child_descriptor = PyList_GetItem(children.get(), index);
        OwnedPyObject<> child {build_node_from_descriptor(child_descriptor, module_name, pending)};
        if (!child ||
            PyObject_SetAttrString(
                type.get(), PyUnicode_AsUTF8(PyDict_GetItemString(child_descriptor, "name")), child.get()) < 0) {
            return nullptr;
        }
    }
    pending.emplace_back(OwnedPyObject<>::fromBorrowed(type.get()), OwnedPyObject<>::fromBorrowed(descriptor));
    return type;
}

OwnedPyObject<> build_node_from_descriptor(
    PyObject* descriptor, const char* module_name, std::vector<std::pair<OwnedPyObject<>, OwnedPyObject<>>>& pending)
{
    PyObject* kind = PyDict_GetItemString(descriptor, "kind");
    if (!kind) {
        return nullptr;
    }
    if (PyUnicode_CompareWithASCIIString(kind, "enum") == 0) {
        return create_enum_type(descriptor, module_name);
    }
    return create_struct_type(descriptor, module_name, pending);
}

bool finalize_pending_types(const std::vector<std::pair<OwnedPyObject<>, OwnedPyObject<>>>& pending)
{
    for (const auto& item: pending) {
        PyObject* type       = item.first.get();
        PyObject* descriptor = item.second.get();
        OwnedPyObject<> enum_fields {PyObject_GetAttrString(type, "_enum_fields")};
        OwnedPyObject<> list_enum_fields {PyObject_GetAttrString(type, "_list_enum_fields")};
        if (!enum_fields || !list_enum_fields) {
            return false;
        }
        PyObject* key                    = nullptr;
        PyObject* value                  = nullptr;
        Py_ssize_t position              = 0;
        PyObject* descriptor_enum_fields = PyDict_GetItemString(descriptor, "enum_fields");
        while (PyDict_Next(descriptor_enum_fields, &position, &key, &value)) {
            auto* state = get_module_state();
            if (!state) {
                return false;
            }
            auto it = state->enum_registry.find(PyLong_AsUnsignedLongLong(value));
            if (it == state->enum_registry.end() || PyDict_SetItem(enum_fields.get(), key, it->second.get()) < 0) {
                return false;
            }
        }
        position                              = 0;
        PyObject* descriptor_list_enum_fields = PyDict_GetItemString(descriptor, "list_enum_fields");
        while (PyDict_Next(descriptor_list_enum_fields, &position, &key, &value)) {
            auto* state = get_module_state();
            if (!state) {
                return false;
            }
            auto it = state->enum_registry.find(PyLong_AsUnsignedLongLong(value));
            if (it == state->enum_registry.end() || PyDict_SetItem(list_enum_fields.get(), key, it->second.get()) < 0) {
                return false;
            }
        }
    }
    return true;
}

}  // namespace

OwnedPyObject<> get_type_by_schema_id(uint64_t schema_id)
{
    auto* state = get_module_state();
    if (!state) {
        PyErr_SetString(PyExc_RuntimeError, "capnp module state is unavailable");
        return {};
    }
    auto it = state->type_registry.find(schema_id);
    if (it == state->type_registry.end()) {
        PyErr_SetString(PyExc_KeyError, "unknown protocol struct schema id");
        return {};
    }
    return it->second;
}

OwnedPyObject<> get_enum_by_schema_id(uint64_t schema_id)
{
    auto* state = get_module_state();
    if (!state) {
        PyErr_SetString(PyExc_RuntimeError, "capnp module state is unavailable");
        return {};
    }
    auto it = state->enum_registry.find(schema_id);
    if (it == state->enum_registry.end()) {
        PyErr_SetString(PyExc_KeyError, "unknown protocol enum schema id");
        return {};
    }
    return it->second;
}

OwnedPyObject<> get_enum_field_value_type()
{
    auto* state = get_module_state();
    if (!state || !state->enum_field_value_type) {
        return {};
    }
    return state->enum_field_value_type;
}

OwnedPyObject<> get_module_descriptor(const char* module_name)
{
    auto* state = get_module_state();
    if (!state || !state->schema_registry.init()) {
        return {};
    }
    auto schemas = state->schema_registry.getModuleSchemas(module_name);
    if (!schemas) {
        PyErr_SetString(PyExc_KeyError, "unknown protocol schema module");
        return {};
    }
    OwnedPyObject<> result {PyList_New(0)};
    if (!result) {
        return nullptr;
    }
    for (auto schema: *schemas) {
        OwnedPyObject<> descriptor {build_schema_descriptor(schema)};
        if (!descriptor || PyList_Append(result.get(), descriptor.get()) < 0) {
            return nullptr;
        }
    }
    return result;
}

bool initialize_runtime_modules(PyObject* module)
{
    auto* state = get_module_state(module);
    if (!state) {
        PyErr_SetString(PyExc_RuntimeError, "capnp module state is unavailable");
        return false;
    }

    OwnedPyObject<> runtime_initialized {PyObject_GetAttrString(module, "_runtime_initialized")};
    if (runtime_initialized && PyObject_IsTrue(runtime_initialized.get()) == 1) {
        return true;
    }
    PyErr_Clear();

    OwnedPyObject<> enum_module {PyImport_ImportModule("enum")};
    if (!enum_module) {
        return false;
    }
    state->enum_class = PyObject_GetAttrString(enum_module.get(), "Enum");
    if (!state->enum_class) {
        return false;
    }

    OwnedPyObject<> base_module {PyModule_New("scaler.protocol.python._base")};
    if (!base_module || !register_module(base_module.get(), "scaler.protocol.python._base")) {
        return false;
    }

    OwnedPyObject<> empty_bases {PyTuple_Pack(1, (PyObject*)&PyBaseObject_Type)};
    OwnedPyObject<> enum_field_value_dict {PyDict_New()};
    OwnedPyObject<> capnp_struct_dict {PyDict_New()};
    OwnedPyObject<> capnp_union_struct_dict {PyDict_New()};
    if (!empty_bases || !enum_field_value_dict || !capnp_struct_dict || !capnp_union_struct_dict) {
        return false;
    }

    PyDict_SetItemString(
        enum_field_value_dict.get(),
        "__module__",
        OwnedPyObject<>(PyUnicode_FromString("scaler.protocol.python.capnp")).get());
    OwnedPyObject<> enum_field_value_type {
        create_python_class("EnumFieldValue", empty_bases.get(), enum_field_value_dict.get())};
    if (!enum_field_value_type) {
        return false;
    }
    Py_INCREF(enum_field_value_type.get());
    state->enum_field_value_type = enum_field_value_type.get();
    PyObject_SetAttrString(
        enum_field_value_type.get(),
        "__init__",
        OwnedPyObject<>(make_method_descriptor(enum_field_value_type.get(), &ENUM_FIELD_VALUE_INIT_DEF)).get());
    PyObject_SetAttrString(
        enum_field_value_type.get(),
        "_as_str",
        OwnedPyObject<>(make_method_descriptor(enum_field_value_type.get(), &ENUM_FIELD_VALUE_AS_STR_DEF)).get());
    PyObject_SetAttrString(
        enum_field_value_type.get(),
        "__eq__",
        OwnedPyObject<>(make_method_descriptor(enum_field_value_type.get(), &ENUM_FIELD_VALUE_EQ_DEF)).get());
    PyObject_SetAttrString(
        enum_field_value_type.get(),
        "__hash__",
        OwnedPyObject<>(make_method_descriptor(enum_field_value_type.get(), &ENUM_FIELD_VALUE_HASH_DEF)).get());
    PyObject_SetAttrString(
        enum_field_value_type.get(),
        "__int__",
        OwnedPyObject<>(make_method_descriptor(enum_field_value_type.get(), &ENUM_FIELD_VALUE_INT_DEF)).get());

    PyDict_SetItemString(
        capnp_struct_dict.get(),
        "__module__",
        OwnedPyObject<>(PyUnicode_FromString("scaler.protocol.python.capnp")).get());
    PyDict_SetItemString(capnp_struct_dict.get(), "_enum_fields", OwnedPyObject<>(PyDict_New()).get());
    PyDict_SetItemString(capnp_struct_dict.get(), "_list_enum_fields", OwnedPyObject<>(PyDict_New()).get());
    PyDict_SetItemString(
        capnp_struct_dict.get(), "from_bytes", OwnedPyObject<>(make_class_method(&CAPNP_STRUCT_FROM_BYTES_DEF)).get());
    OwnedPyObject<> capnp_struct_type {create_python_class("CapnpStruct", empty_bases.get(), capnp_struct_dict.get())};
    if (!capnp_struct_type) {
        return false;
    }
    Py_INCREF(capnp_struct_type.get());
    state->capnp_struct_type = capnp_struct_type.get();
    PyObject_SetAttrString(
        capnp_struct_type.get(),
        "__init__",
        OwnedPyObject<>(make_method_descriptor(capnp_struct_type.get(), &CAPNP_STRUCT_INIT_DEF)).get());
    PyObject_SetAttrString(
        capnp_struct_type.get(),
        "to_bytes",
        OwnedPyObject<>(make_method_descriptor(capnp_struct_type.get(), &CAPNP_STRUCT_TO_BYTES_DEF)).get());

    OwnedPyObject<> union_bases {PyTuple_Pack(1, capnp_struct_type.get())};
    PyDict_SetItemString(
        capnp_union_struct_dict.get(),
        "__module__",
        OwnedPyObject<>(PyUnicode_FromString("scaler.protocol.python.capnp")).get());
    PyDict_SetItemString(capnp_union_struct_dict.get(), "_union_fields", OwnedPyObject<>(PySet_New(nullptr)).get());
    PyDict_SetItemString(
        capnp_union_struct_dict.get(),
        "from_bytes",
        OwnedPyObject<>(make_class_method(&CAPNP_UNION_FROM_BYTES_DEF)).get());
    OwnedPyObject<> capnp_union_struct_type {
        create_python_class("CapnpUnionStruct", union_bases.get(), capnp_union_struct_dict.get())};
    if (!capnp_union_struct_type) {
        return false;
    }
    Py_INCREF(capnp_union_struct_type.get());
    state->capnp_union_struct_type = capnp_union_struct_type.get();
    PyObject_SetAttrString(
        capnp_union_struct_type.get(),
        "__init__",
        OwnedPyObject<>(make_method_descriptor(capnp_union_struct_type.get(), &CAPNP_UNION_INIT_DEF)).get());
    PyObject_SetAttrString(
        capnp_union_struct_type.get(),
        "which",
        OwnedPyObject<>(make_method_descriptor(capnp_union_struct_type.get(), &CAPNP_UNION_WHICH_DEF)).get());
    PyObject_SetAttrString(
        capnp_union_struct_type.get(),
        "__getattr__",
        OwnedPyObject<>(make_method_descriptor(capnp_union_struct_type.get(), &CAPNP_UNION_GETATTR_DEF)).get());
    PyObject_SetAttrString(
        capnp_union_struct_type.get(),
        "to_bytes",
        OwnedPyObject<>(make_method_descriptor(capnp_union_struct_type.get(), &CAPNP_UNION_TO_BYTES_DEF)).get());

    PyModule_AddObjectRef(base_module.get(), "EnumFieldValue", enum_field_value_type.get());
    PyModule_AddObjectRef(base_module.get(), "CapnpStruct", capnp_struct_type.get());
    PyModule_AddObjectRef(base_module.get(), "CapnpUnionStruct", capnp_union_struct_type.get());
    PyModule_AddObjectRef(module, "EnumFieldValue", enum_field_value_type.get());
    PyModule_AddObjectRef(module, "CapnpStruct", capnp_struct_type.get());
    PyModule_AddObjectRef(module, "CapnpUnionStruct", capnp_union_struct_type.get());

    for (const char* short_module_name: {"common", "status", "object_storage", "message"}) {
        OwnedPyObject<> descriptors {get_module_descriptor(short_module_name)};
        if (!descriptors) {
            return false;
        }
        std::string full_module_name = std::string("scaler.protocol.python._") + short_module_name;
        OwnedPyObject<> generated_module {PyModule_New(full_module_name.c_str())};
        if (!generated_module || !register_module(generated_module.get(), full_module_name.c_str())) {
            return false;
        }
        std::vector<std::pair<OwnedPyObject<>, OwnedPyObject<>>> pending;
        OwnedPyObject<> all_list {PyList_New(0)};
        if (!all_list) {
            return false;
        }
        Py_ssize_t descriptor_count = PyList_Size(descriptors.get());
        for (Py_ssize_t index = 0; index < descriptor_count; ++index) {
            PyObject* descriptor = PyList_GetItem(descriptors.get(), index);
            OwnedPyObject<> object {build_node_from_descriptor(descriptor, full_module_name.c_str(), pending)};
            if (!object) {
                return false;
            }
            PyObject* name = PyDict_GetItemString(descriptor, "name");
            if (PyObject_SetAttr(generated_module.get(), name, object.get()) < 0 ||
                PyList_Append(all_list.get(), name) < 0) {
                return false;
            }
            if (PyObject_HasAttr(module, name) == 0 && PyObject_SetAttr(module, name, object.get()) < 0) {
                return false;
            }
        }
        if (!finalize_pending_types(pending)) {
            return false;
        }
        if (PyObject_SetAttrString(generated_module.get(), "__all__", all_list.get()) < 0 ||
            PyObject_SetAttrString(module, (std::string("_") + short_module_name).c_str(), generated_module.get()) <
                0) {
            return false;
        }
    }

    return PyObject_SetAttrString(module, "_runtime_initialized", Py_True) == 0;
}

}  // namespace scaler::protocol::pymod

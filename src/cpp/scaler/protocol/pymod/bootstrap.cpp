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

// Wasm-relocator-safe attribute and key names. See the equivalent block at the
// top of utility.cpp and the long comment in initialize_runtime_modules() for
// why bare ``"foo"`` literals are unsafe in this translation unit when built
// for Pyodide / Emscripten SIDE_MODULE.
static const char DESC_KIND[]                 = "kind";
static const char DESC_NAME[]                 = "name";
static const char DESC_ID[]                   = "id";
static const char DESC_MEMBERS[]              = "members";
static const char DESC_ENUM_FIELDS[]          = "enum_fields";
static const char DESC_LIST_ENUM_FIELDS[]     = "list_enum_fields";
static const char DESC_UNION_FIELDS[]         = "union_fields";
static const char DESC_CHILDREN[]             = "children";
static const char ATTR_MODULE_DUNDER[]        = "__module__";
static const char ATTR_SCHEMA_NODE_ID[]       = "_schema_node_id";
static const char ATTR_ENUM_FIELDS[]          = "_enum_fields";
static const char ATTR_LIST_ENUM_FIELDS[]     = "_list_enum_fields";
static const char ATTR_UNION_FIELDS[]         = "_union_fields";
static const char ATTR_RUNTIME_INITIALIZED[]  = "_runtime_initialized";
static const char ATTR_FROM_BYTES[]           = "from_bytes";
static const char ATTR_ALL_DUNDER[]           = "__all__";
static const char SCALER_PROTOCOL_CAPNP[]     = "scaler.protocol.capnp";

OwnedPyObject<> build_schema_descriptor(capnp::Schema schema)
{
    auto proto = schema.getProto();

    OwnedPyObject<> descriptor {PyDict_New()};
    if (!descriptor) {
        return nullptr;
    }

    static const char KIND_ENUM[]   = {'e','n','u','m','\0'};
    static const char KIND_STRUCT[] = {'s','t','r','u','c','t','\0'};
    const char* kind = proto.isEnum() ? KIND_ENUM : KIND_STRUCT;
    PyDict_SetItemString(descriptor.get(), DESC_KIND, OwnedPyObject<>(PyUnicode_FromString(kind)).get());
    auto unqualified = schema.getUnqualifiedName();
    PyDict_SetItemString(descriptor.get(), DESC_NAME, OwnedPyObject<>(PyUnicode_FromString(unqualified.cStr())).get());
    PyDict_SetItemString(descriptor.get(), DESC_ID, OwnedPyObject<>(PyLong_FromUnsignedLongLong(proto.getId())).get());

    if (proto.isEnum()) {
        auto enum_schema = schema.asEnum();
        OwnedPyObject<> members {PyList_New(0)};
        if (!members) {
            return nullptr;
        }

        for (auto enumerant: enum_schema.getEnumerants()) {
            OwnedPyObject<> name_obj {PyUnicode_FromString(enumerant.getProto().getName().cStr())};
            OwnedPyObject<> ord_obj {PyLong_FromUnsignedLong((unsigned long)enumerant.getOrdinal())};
            if (!name_obj || !ord_obj) {
                return nullptr;
            }
            OwnedPyObject<> tuple {PyTuple_Pack(2, name_obj.get(), ord_obj.get())};
            if (!tuple || PyList_Append(members.get(), tuple.get()) < 0) {
                return nullptr;
            }
        }

        PyDict_SetItemString(descriptor.get(), DESC_MEMBERS, members.get());
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

    PyDict_SetItemString(descriptor.get(), DESC_ENUM_FIELDS, enum_fields.get());
    PyDict_SetItemString(descriptor.get(), DESC_LIST_ENUM_FIELDS, list_enum_fields.get());
    PyDict_SetItemString(descriptor.get(), DESC_UNION_FIELDS, union_fields.get());
    PyDict_SetItemString(descriptor.get(), DESC_CHILDREN, children.get());
    return descriptor;
}

OwnedPyObject<> make_builtin_function(const PyMethodDef* def)
{
    return OwnedPyObject<> {PyCFunction_NewEx(const_cast<PyMethodDef*>(def), nullptr, nullptr)};
}

OwnedPyObject<> make_class_method(const PyMethodDef* def)
{
    OwnedPyObject<> function {make_builtin_function(def)};
    if (!function) {
        return {};
    }
    return OwnedPyObject<> {PyClassMethod_New(function.get())};
}

OwnedPyObject<> make_method_descriptor(PyObject* type, const PyMethodDef* def)
{
    return OwnedPyObject<> {PyDescr_NewMethod((PyTypeObject*)type, const_cast<PyMethodDef*>(def))};
}

bool register_module(PyObject* module, const char* full_name)
{
    return PyDict_SetItemString(PyImport_GetModuleDict(), full_name, module) == 0;
}

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
{
    return ::scaler::protocol::pymod::capnp_struct_init_method(self, args, kwargs).take();
}

PyObject* py_capnp_struct_get_attr(PyObject* self, PyObject* args)
{
    return ::scaler::protocol::pymod::capnp_struct_get_attr(self, args).take();
}

PyObject* py_capnp_struct_to_bytes(PyObject* self, PyObject* /*unused*/)
{
    return ::scaler::protocol::pymod::capnp_struct_to_bytes(self).take();
}

PyObject* py_capnp_struct_from_bytes(PyObject* cls, PyObject* args, PyObject* kwargs)
{
    return ::scaler::protocol::pymod::capnp_struct_from_bytes(cls, args, kwargs).take();
}

PyObject* py_capnp_union_init_method(PyObject* self, PyObject* args, PyObject* kwargs)
{
    return ::scaler::protocol::pymod::capnp_union_init_method(self, args, kwargs).take();
}

PyObject* py_capnp_union_which(PyObject* self, PyObject* /*unused*/)
{
    return ::scaler::protocol::pymod::capnp_union_which(self).take();
}

PyObject* py_capnp_union_get_attr(PyObject* self, PyObject* args)
{
    return ::scaler::protocol::pymod::capnp_union_get_attr(self, args).take();
}

PyObject* py_capnp_union_to_bytes(PyObject* self, PyObject* /*unused*/)
{
    return ::scaler::protocol::pymod::capnp_union_to_bytes(self).take();
}

PyObject* py_capnp_union_from_bytes(PyObject* cls, PyObject* args, PyObject* kwargs)
{
    return ::scaler::protocol::pymod::capnp_union_from_bytes(cls, args, kwargs).take();
}

// initproc adapters wired directly into tp_init. See note in
// initialize_runtime_modules() on why these bypass the descriptor machinery.
int capnp_struct_init_slot(PyObject* self, PyObject* args, PyObject* kwargs)
{
    auto result = ::scaler::protocol::pymod::capnp_struct_init_method(self, args, kwargs);
    return result ? 0 : -1;
}

int capnp_union_init_slot(PyObject* self, PyObject* args, PyObject* kwargs)
{
    auto result = ::scaler::protocol::pymod::capnp_union_init_method(self, args, kwargs);
    return result ? 0 : -1;
}

// PyMethodDef::ml_name and the keys passed to PyObject_SetAttrString below are
// stored as ``static const char[]`` arrays rather than inline string literals.
// Pyodide's SIDE_MODULE wasm relocator can mis-resolve offsets within mergeable
// .rodata.str sections, corrupting short literals like "__init__" so that the
// resulting descriptor is registered under the wrong attribute name and
// ``Resource(cpu=1, rss=2)`` falls through to ``object.__init__`` (which raises
// ``TypeError: Resource() takes no arguments``). Named char arrays survive the
// relocator. Keep them in lockstep with the keys used in setattr below.
static const char NAME_INIT[]       = {'_', '_', 'i', 'n', 'i', 't', '_', '_', '\0'};
static const char NAME_TO_BYTES[]   = {'t', 'o', '_', 'b', 'y', 't', 'e', 's', '\0'};
static const char NAME_FROM_BYTES[] = {'f', 'r', 'o', 'm', '_', 'b', 'y', 't', 'e', 's', '\0'};
static const char NAME_WHICH[]      = {'w', 'h', 'i', 'c', 'h', '\0'};
static const char NAME_GETATTR[]    = {'_', '_', 'g', 'e', 't', 'a', 't', 't', 'r', '_', '_', '\0'};

static PyMethodDef CAPNP_STRUCT_INIT_DEF     = {
    NAME_INIT, (PyCFunction)(void (*)(void))py_capnp_struct_init_method, METH_VARARGS | METH_KEYWORDS, nullptr};
static PyMethodDef CAPNP_STRUCT_GETATTR_DEF = {
    NAME_GETATTR, (PyCFunction)py_capnp_struct_get_attr, METH_VARARGS, nullptr};
static PyMethodDef CAPNP_STRUCT_TO_BYTES_DEF = {
    NAME_TO_BYTES, (PyCFunction)py_capnp_struct_to_bytes, METH_NOARGS, nullptr};
static PyMethodDef CAPNP_STRUCT_FROM_BYTES_DEF = {
    NAME_FROM_BYTES, (PyCFunction)(void (*)(void))py_capnp_struct_from_bytes, METH_VARARGS | METH_KEYWORDS, nullptr};
static PyMethodDef CAPNP_UNION_INIT_DEF = {
    NAME_INIT, (PyCFunction)(void (*)(void))py_capnp_union_init_method, METH_VARARGS | METH_KEYWORDS, nullptr};
static PyMethodDef CAPNP_UNION_WHICH_DEF   = {NAME_WHICH, (PyCFunction)py_capnp_union_which, METH_NOARGS, nullptr};
static PyMethodDef CAPNP_UNION_GETATTR_DEF = {
    NAME_GETATTR, (PyCFunction)py_capnp_union_get_attr, METH_VARARGS, nullptr};
static PyMethodDef CAPNP_UNION_TO_BYTES_DEF = {NAME_TO_BYTES, (PyCFunction)py_capnp_union_to_bytes, METH_NOARGS, nullptr};
static PyMethodDef CAPNP_UNION_FROM_BYTES_DEF = {
    NAME_FROM_BYTES, (PyCFunction)(void (*)(void))py_capnp_union_from_bytes, METH_VARARGS | METH_KEYWORDS, nullptr};

OwnedPyObject<> create_enum_type(PyObject* descriptor, const char* module_name)
{
    OwnedPyObject<> name {Py_NewRef(PyDict_GetItemString(descriptor, DESC_NAME))};
    OwnedPyObject<> members_list {Py_NewRef(PyDict_GetItemString(descriptor, DESC_MEMBERS))};
    OwnedPyObject<> schema_id_obj {Py_NewRef(PyDict_GetItemString(descriptor, DESC_ID))};
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
    OwnedPyObject<> module_name_obj {PyUnicode_FromString(module_name)};
    if (!module_name_obj) {
        return nullptr;
    }
    // Pass `module=` explicitly so enum.Enum's functional API does not attempt
    // to infer __module__ via sys._getframe inspection (which fails when the
    // caller is a C extension and, on Python 3.13+, ends up triggering an
    // empty-name __import__("") that raises ValueError("Empty module name")).
    OwnedPyObject<> kwargs {PyDict_New()};
    static const char MODULE_KW[] = {'m','o','d','u','l','e','\0'};
    if (!kwargs || PyDict_SetItemString(kwargs.get(), MODULE_KW, module_name_obj.get()) < 0) {
        return nullptr;
    }
    OwnedPyObject<> args {PyTuple_Pack(2, name.get(), members_dict.get())};
    if (!args) {
        return nullptr;
    }
    OwnedPyObject<> enum_type {PyObject_Call(state->enum_class.get(), args.get(), kwargs.get())};
    if (!enum_type) {
        return nullptr;
    }
    if (PyObject_SetAttrString(enum_type.get(), ATTR_MODULE_DUNDER, module_name_obj.get()) < 0 ||
        PyObject_SetAttrString(enum_type.get(), ATTR_SCHEMA_NODE_ID, schema_id_obj.get()) < 0) {
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
    OwnedPyObject<> name {Py_NewRef(PyDict_GetItemString(descriptor, DESC_NAME))};
    OwnedPyObject<> schema_id_obj {Py_NewRef(PyDict_GetItemString(descriptor, DESC_ID))};
    OwnedPyObject<> union_fields {Py_NewRef(PyDict_GetItemString(descriptor, DESC_UNION_FIELDS))};
    OwnedPyObject<> children {Py_NewRef(PyDict_GetItemString(descriptor, DESC_CHILDREN))};
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
    PyDict_SetItemString(dict.get(), ATTR_MODULE_DUNDER, module_name_obj.get());
    PyDict_SetItemString(dict.get(), ATTR_SCHEMA_NODE_ID, schema_id_obj.get());
    PyDict_SetItemString(dict.get(), ATTR_ENUM_FIELDS, enum_fields.get());
    PyDict_SetItemString(dict.get(), ATTR_LIST_ENUM_FIELDS, list_enum_fields.get());
    PyDict_SetItemString(dict.get(), ATTR_UNION_FIELDS, union_field_set.get());
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
                type.get(), PyUnicode_AsUTF8(PyDict_GetItemString(child_descriptor, DESC_NAME)), child.get()) < 0) {
            return nullptr;
        }
    }
    pending.emplace_back(OwnedPyObject<>::fromBorrowed(type.get()), OwnedPyObject<>::fromBorrowed(descriptor));
    return type;
}

OwnedPyObject<> build_node_from_descriptor(
    PyObject* descriptor, const char* module_name, std::vector<std::pair<OwnedPyObject<>, OwnedPyObject<>>>& pending)
{
    PyObject* kind = PyDict_GetItemString(descriptor, DESC_KIND);
    if (!kind) {
        return nullptr;
    }
    static const char ENUM_KIND[] = {'e','n','u','m','\0'};
    int is_enum = PyUnicode_CompareWithASCIIString(kind, ENUM_KIND);
    if (is_enum == 0) {
        return create_enum_type(descriptor, module_name);
    }
    auto r = create_struct_type(descriptor, module_name, pending);
    return r;
}

bool finalize_pending_types(const std::vector<std::pair<OwnedPyObject<>, OwnedPyObject<>>>& pending)
{
    for (const auto& item: pending) {
        PyObject* type       = item.first.get();
        PyObject* descriptor = item.second.get();
        OwnedPyObject<> enum_fields {PyObject_GetAttrString(type, ATTR_ENUM_FIELDS)};
        OwnedPyObject<> list_enum_fields {PyObject_GetAttrString(type, ATTR_LIST_ENUM_FIELDS)};
        if (!enum_fields || !list_enum_fields) {
            return false;
        }
        PyObject* key                    = nullptr;
        PyObject* value                  = nullptr;
        Py_ssize_t position              = 0;
        PyObject* descriptor_enum_fields = PyDict_GetItemString(descriptor, DESC_ENUM_FIELDS);
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
        PyObject* descriptor_list_enum_fields = PyDict_GetItemString(descriptor, DESC_LIST_ENUM_FIELDS);
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
    // NOTE: Many strings used as module names, attribute keys, etc. below are declared as
    // `static const char NAME[]` arrays rather than inline string literals. This is required
    // for Pyodide/Emscripten SIDE_MODULE builds: the wasm relocator can mis-resolve offsets
    // within mergeable .rodata.str sections, which corrupts (truncates / misaligns) raw string
    // literals passed to the CPython C API. Using named static char arrays forces the compiler
    // to emit them as ordinary symbols that survive relocation. Build flags additionally pass
    // -fno-merge-all-constants to discourage merging. Do not convert these back to bare string
    // literals without testing the wasm wheel.

    auto* state = get_module_state(module);
    if (!state) {
        PyErr_SetString(PyExc_RuntimeError, "capnp module state is unavailable");
        return false;
    }

    OwnedPyObject<> runtime_initialized {PyObject_GetAttrString(module, ATTR_RUNTIME_INITIALIZED)};
    if (runtime_initialized && PyObject_IsTrue(runtime_initialized.get()) == 1) {
        return true;
    }
    PyErr_Clear();

    static const char enum_name[] = {'e', 'n', 'u', 'm', '\0'};
    OwnedPyObject<> enum_module {PyImport_ImportModule(enum_name)};
    if (!enum_module) {
        return false;
    }
    static const char enum_attr_name[] = {'I', 'n', 't', 'E', 'n', 'u', 'm', '\0'};
    state->enum_class                  = PyObject_GetAttrString(enum_module.get(), enum_attr_name);
    if (!state->enum_class) {
        return false;
    }

    OwnedPyObject<> base_module {PyModule_New("scaler.protocol._base")};
    if (!base_module || !register_module(base_module.get(), "scaler.protocol._base")) {
        return false;
    }

    OwnedPyObject<> empty_bases {PyTuple_Pack(1, (PyObject*)&PyBaseObject_Type)};
    OwnedPyObject<> capnp_struct_dict {PyDict_New()};
    OwnedPyObject<> capnp_union_struct_dict {PyDict_New()};
    if (!empty_bases || !capnp_struct_dict || !capnp_union_struct_dict) {
        return false;
    }

    PyDict_SetItemString(
        capnp_struct_dict.get(),
        ATTR_MODULE_DUNDER,
        OwnedPyObject<>(PyUnicode_FromString(SCALER_PROTOCOL_CAPNP)).get());
    PyDict_SetItemString(capnp_struct_dict.get(), ATTR_ENUM_FIELDS, OwnedPyObject<>(PyDict_New()).get());
    PyDict_SetItemString(capnp_struct_dict.get(), ATTR_LIST_ENUM_FIELDS, OwnedPyObject<>(PyDict_New()).get());
    PyDict_SetItemString(
        capnp_struct_dict.get(),
        ATTR_FROM_BYTES,
        OwnedPyObject<>(make_class_method(&CAPNP_STRUCT_FROM_BYTES_DEF)).get());
    OwnedPyObject<> capnp_struct_type {create_python_class("CapnpStruct", empty_bases.get(), capnp_struct_dict.get())};
    if (!capnp_struct_type) {
        return false;
    }
    Py_INCREF(capnp_struct_type.get());
    state->capnp_struct_type = capnp_struct_type.get();
    PyObject_SetAttrString(
        capnp_struct_type.get(),
        NAME_INIT,
        OwnedPyObject<>(make_method_descriptor(capnp_struct_type.get(), &CAPNP_STRUCT_INIT_DEF)).get());
    PyObject_SetAttrString(
        capnp_struct_type.get(),
        NAME_GETATTR,
        OwnedPyObject<>(make_method_descriptor(capnp_struct_type.get(), &CAPNP_STRUCT_GETATTR_DEF)).get());
    PyObject_SetAttrString(
        capnp_struct_type.get(),
        NAME_TO_BYTES,
        OwnedPyObject<>(make_method_descriptor(capnp_struct_type.get(), &CAPNP_STRUCT_TO_BYTES_DEF)).get());
    // Belt-and-suspenders: also set the tp_init slot directly so that subclasses
    // created later via type(name, (CapnpStruct,), {}) inherit the C initproc
    // even if PyObject_SetAttrString's slot-resync logic misbehaves under the
    // Pyodide SIDE_MODULE relocator. Subclass slot inheritance happens at
    // type-creation time via inherit_slots() in CPython's type_new, so this
    // must run before any struct subclasses are created.
    ((PyTypeObject*)capnp_struct_type.get())->tp_init = capnp_struct_init_slot;
    PyType_Modified((PyTypeObject*)capnp_struct_type.get());

    OwnedPyObject<> union_bases {PyTuple_Pack(1, capnp_struct_type.get())};
    PyDict_SetItemString(
        capnp_union_struct_dict.get(),
        ATTR_MODULE_DUNDER,
        OwnedPyObject<>(PyUnicode_FromString(SCALER_PROTOCOL_CAPNP)).get());
    PyDict_SetItemString(capnp_union_struct_dict.get(), ATTR_UNION_FIELDS, OwnedPyObject<>(PySet_New(nullptr)).get());
    PyDict_SetItemString(
        capnp_union_struct_dict.get(),
        ATTR_FROM_BYTES,
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
        NAME_INIT,
        OwnedPyObject<>(make_method_descriptor(capnp_union_struct_type.get(), &CAPNP_UNION_INIT_DEF)).get());
    PyObject_SetAttrString(
        capnp_union_struct_type.get(),
        NAME_WHICH,
        OwnedPyObject<>(make_method_descriptor(capnp_union_struct_type.get(), &CAPNP_UNION_WHICH_DEF)).get());
    PyObject_SetAttrString(
        capnp_union_struct_type.get(),
        NAME_GETATTR,
        OwnedPyObject<>(make_method_descriptor(capnp_union_struct_type.get(), &CAPNP_UNION_GETATTR_DEF)).get());
    PyObject_SetAttrString(
        capnp_union_struct_type.get(),
        NAME_TO_BYTES,
        OwnedPyObject<>(make_method_descriptor(capnp_union_struct_type.get(), &CAPNP_UNION_TO_BYTES_DEF)).get());
    // See note above on direct tp_init assignment. CapnpUnionStruct subclasses
    // (created by the loop below for union-bearing structs) need the union
    // initproc, not the plain struct one inherited from CapnpStruct.
    ((PyTypeObject*)capnp_union_struct_type.get())->tp_init = capnp_union_init_slot;
    PyType_Modified((PyTypeObject*)capnp_union_struct_type.get());

    static const char ATTR_CAPNP_STRUCT[]       = "CapnpStruct";
    static const char ATTR_CAPNP_UNION_STRUCT[] = "CapnpUnionStruct";
    static const char ATTR_BASE_MESSAGE[]       = "BaseMessage";
    PyModule_AddObjectRef(base_module.get(), ATTR_CAPNP_STRUCT, capnp_struct_type.get());
    PyModule_AddObjectRef(base_module.get(), ATTR_CAPNP_UNION_STRUCT, capnp_union_struct_type.get());
    PyModule_AddObjectRef(module, ATTR_BASE_MESSAGE, capnp_struct_type.get());

    // Use lowercase-prefixed identifiers (not MOD_*) to avoid colliding with
    // platform/system macros (e.g. some Linux/Python headers preprocess
    // ``MOD_STATUS`` into a numeric constant, breaking the build).
    static const char kModCommon[]        = "common";
    static const char kModStatus[]        = "status";
    static const char kModObjectStorage[] = "object_storage";
    static const char kModMessage[]       = "message";
    const char* short_module_names[4];
    short_module_names[0] = kModCommon;
    short_module_names[1] = kModStatus;
    short_module_names[2] = kModObjectStorage;
    short_module_names[3] = kModMessage;
    for (const char* short_module_name: short_module_names) {
        OwnedPyObject<> descriptors {get_module_descriptor(short_module_name)};
        if (!descriptors) {
            return false;
        }
        std::string full_module_name = std::string("scaler.protocol._") + short_module_name;
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
            PyObject* name = PyDict_GetItemString(descriptor, DESC_NAME);
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
        if (PyObject_SetAttrString(generated_module.get(), ATTR_ALL_DUNDER, all_list.get()) < 0) {
            return false;
        }
    }

    return PyObject_SetAttrString(module, ATTR_RUNTIME_INITIALIZED, Py_True) == 0;
}

}  // namespace scaler::protocol::pymod

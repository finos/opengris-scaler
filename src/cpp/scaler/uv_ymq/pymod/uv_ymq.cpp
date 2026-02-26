#include "scaler/uv_ymq/pymod/uv_ymq.h"

#include <new>
#include <vector>

#include "scaler/uv_ymq/pymod/address.h"
#include "scaler/uv_ymq/pymod/io_context.h"

namespace scaler {
namespace uv_ymq {
namespace pymod {

UVYMQState* UVYMQStateFromSelf(PyObject* self)
{
    PyObject* pyModule = PyType_GetModule(Py_TYPE(self));
    if (!pyModule)
        return nullptr;

#if PY_MAJOR_VERSION == 3 && PY_MINOR_VERSION == 8
    Py_DECREF(pyModule);  // As we get a real ref in 3.8 backport
#endif

    return (UVYMQState*)PyModule_GetState(pyModule);
}

void UVYMQ_free(void* stateVoid)
{
    UVYMQState* state = (UVYMQState*)stateVoid;
    if (state) {
        state->~UVYMQState();
    }
}

int UVYMQ_createIntEnum(
    PyObject* pyModule,
    OwnedPyObject<>* storage,
    std::string enumName,
    std::vector<std::pair<std::string, int>> entries)
{
    // create a python dictionary to hold the entries
    OwnedPyObject enumDict = PyDict_New();
    if (!enumDict)
        return -1;

    // add each entry to the dictionary
    for (const auto& entry: entries) {
        OwnedPyObject value = PyLong_FromLong(entry.second);
        if (!value)
            return -1;

        auto status = PyDict_SetItemString(*enumDict, entry.first.c_str(), *value);
        if (status < 0)
            return -1;
    }

    auto state = (UVYMQState*)PyModule_GetState(pyModule);

    if (!state)
        return -1;

    // create our class by calling enum.IntEnum(enumName, enumDict)
    OwnedPyObject enumClass = PyObject_CallMethod(*state->enumModule, "IntEnum", "sO", enumName.c_str(), *enumDict);
    if (!enumClass)
        return -1;

    *storage = enumClass;

    // add the class to the module
    // this increments the reference count of enumClass
    return PyModule_AddObjectRef(pyModule, enumName.c_str(), *enumClass);
}

static int UVYMQ_createType(
    PyObject* pyModule,
    OwnedPyObject<>* storage,
    PyType_Spec* spec,
    const char* name,
    bool add,
    PyObject* bases,
    getbufferproc getbuffer,
    releasebufferproc releasebuffer)
{
    assert(storage != nullptr);

    *storage = PyType_FromModuleAndSpec(pyModule, spec, bases);
    if (!*storage)
        return -1;

#if PY_MAJOR_VERSION == 3 && PY_MINOR_VERSION == 8
    if (PyObject_SetAttrString(**storage, "__module_object__", pyModule) < 0)
        return -1;

    if (getbuffer && releasebuffer) {
        PyTypeObject* type_obj = (PyTypeObject*)**storage;

        type_obj->tp_as_buffer->bf_getbuffer     = getbuffer;
        type_obj->tp_as_buffer->bf_releasebuffer = releasebuffer;
        type_obj->tp_flags |= 0;  // Do I need to add tp_flags? Seems not
    }
#endif

    if (add)
        if (PyModule_AddObjectRef(pyModule, name, **storage) < 0)
            return -1;

    return 0;
}

static int UVYMQ_exec(PyObject* pyModule)
{
    auto state = (UVYMQState*)PyModule_GetState(pyModule);
    if (!state)
        return -1;

    // Use placement new to initialize C++ objects in the pre-allocated (zero-initialized) memory
    new (state) UVYMQState();

    state->enumModule = PyImport_ImportModule("enum");
    if (!state->enumModule)
        return -1;

    if (PyAddressType_createEnum(pyModule, state) < 0)
        return -1;

    if (UVYMQ_createType(pyModule, &state->PyAddressType, &PyAddress_spec, "Address") < 0)
        return -1;

    if (UVYMQ_createType(pyModule, &state->PyIOContextType, &PyIOContext_spec, "IOContext") < 0)
        return -1;

    return 0;
}

}  // namespace pymod
}  // namespace uv_ymq
}  // namespace scaler

PyMODINIT_FUNC PyInit__uv_ymq(void)
{
    return PyModuleDef_Init(&scaler::uv_ymq::pymod::UVYMQ_module);
}

#include "scaler/utility/pymod/compatibility.h"

namespace scaler {
namespace utility {
namespace pending_call {
namespace pymod {

using scaler::utility::pymod::OwnedPyObject;

extern "C" {

// Trampoline invoked by the CPython eval loop on the main interpreter thread,
// at the next eval-breaker check, with the GIL held.
//
// Takes ownership of one strong reference to the callable (transferred via the
// void* arg by PyPendingCallSchedule). Errors raised inside the callable cannot
// propagate from a pending call, so we surface them via PyErr_WriteUnraisable
// and always return 0.
static int PyPendingCallTrampoline(void* arg)
{
    OwnedPyObject<> callable {(PyObject*)arg};

    OwnedPyObject<> result {PyObject_CallNoArgs(callable.get())};
    if (!result) {
        PyErr_WriteUnraisable(callable.get());
    }

    return 0;
}

static PyObject* PyPendingCallSchedule(PyObject* /*self*/, PyObject* callable)
{
    if (!PyCallable_Check(callable)) {
        PyErr_SetString(PyExc_TypeError, "schedule() argument must be callable");
        return nullptr;
    }

    OwnedPyObject<> owned = OwnedPyObject<>::fromBorrowed(callable);
    if (Py_AddPendingCall(PyPendingCallTrampoline, owned.get()) < 0) {
        PyErr_SetString(PyExc_RuntimeError, "Py_AddPendingCall queue is full");
        return nullptr;
    }
    // Successfully queued: ownership of the strong reference transfers to the trampoline through
    // the void* arg. Release the OwnedPyObject without decrementing.
    owned.take();

    Py_RETURN_NONE;
}

static PyMethodDef PendingCallMethods[] = {
    {"schedule",
     (PyCFunction)PyPendingCallSchedule,
     METH_O,
     "Schedule a no-argument callable to run on the main interpreter thread "
     "at the next CPython eval-breaker check (the same safe points where "
     "Python signal handlers run). Thread-safe; intended to substitute for "
     "POSIX signal-driven main-thread interruption on Windows."},
    {nullptr, nullptr, 0, nullptr},
};

static PyModuleDef pending_call_module = {
    .m_base     = PyModuleDef_HEAD_INIT,
    .m_name     = "pending_call",
    .m_doc      = PyDoc_STR("Wrapper around CPython's Py_AddPendingCall API."),
    .m_size     = 0,
    .m_methods  = PendingCallMethods,
    .m_slots    = nullptr,
    .m_traverse = nullptr,
    .m_clear    = nullptr,
    .m_free     = nullptr,
};

}  // extern "C"

}  // namespace pymod
}  // namespace pending_call
}  // namespace utility
}  // namespace scaler

PyMODINIT_FUNC PyInit_pending_call(void)
{
    using scaler::utility::pending_call::pymod::pending_call_module;
    return PyModule_Create(&pending_call_module);
}

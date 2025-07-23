#include <cstdlib>

#include "scaler/io/ymq/pymod_ymq/ymq.h"
#include "scaler/io/ymq/error.h"

constexpr inline void ymqUnrecoverableError(scaler::ymq::Error e) {
    PyGILState_STATE gstate = PyGILState_Ensure();
    PyErr_SetString(PyExc_SystemExit, e.what());
    PyErr_WriteUnraisable(nullptr);
    Py_Finalize();

    std::exit(EXIT_FAILURE);
}

PyMODINIT_FUNC PyInit_ymq(void) {
    unrecoverableErrorFunctionHookPtr = ymqUnrecoverableError;

    return PyModuleDef_Init(&ymq_module);
}

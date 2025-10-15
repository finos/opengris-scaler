#include "scaler/io/ymq/pymod_ymq/ymq.h"

#include <cstdlib>

#include "scaler/io/ymq/error.h"

inline void ymqUnrecoverableError(scaler::ymq::Error e)
{
    PyGILState_STATE gstate = PyGILState_Ensure();
    (void)gstate;  // silent the warning
    PyErr_SetString(PyExc_SystemExit, e.what());
    PyErr_WriteUnraisable(nullptr);
    Py_Finalize();

    std::exit(EXIT_FAILURE);
}

PyMODINIT_FUNC PyInit__ymq(void)
{
    unrecoverableErrorFunctionHookPtr = ymqUnrecoverableError;

    return PyModuleDef_Init(&YMQ_module);
}

#include "scaler/ymq/pymod_ymq/ymq.h"

#include <cstdlib>

#include "scaler/error/error.h"

namespace scaler {
namespace ymq {

inline void ymqUnrecoverableError(scaler::ymq::Error e)
{
    PyGILState_STATE gstate = PyGILState_Ensure();
    (void)gstate;  // silent the warning
    PyErr_SetString(PyExc_SystemExit, e.what());
    PyErr_WriteUnraisable(nullptr);
    Py_Finalize();

    std::exit(EXIT_FAILURE);
}

}  // namespace ymq
}  // namespace scaler

PyMODINIT_FUNC PyInit__ymq(void)
{
    unrecoverableErrorFunctionHookPtr = scaler::ymq::ymqUnrecoverableError;

    return PyModuleDef_Init(&scaler::ymq::YMQ_module);
}

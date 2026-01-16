#pragma once

#define PY_SSIZE_T_CLEAN
#include <Python.h>

namespace scaler {
namespace utility {
namespace pymod {

class AcquireGIL {
public:
    AcquireGIL(): _state(PyGILState_Ensure()) {}
    ~AcquireGIL() { PyGILState_Release(_state); }

    AcquireGIL(const AcquireGIL&)            = delete;
    AcquireGIL& operator=(const AcquireGIL&) = delete;
    AcquireGIL(AcquireGIL&&)                 = delete;
    AcquireGIL& operator=(AcquireGIL&&)      = delete;

private:
    PyGILState_STATE _state;
};

}  // namespace pymod
}  // namespace utility
}  // namespace scaler

#pragma once

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <atomic>

namespace scaler {
namespace utility {
namespace pymod {

// Set to true once the interpreter has begun shutting down.
//
// From that point on, CPython (up to and including 3.13) kills any non-Python thread that calls
// PyGILState_Ensure(), by way of pthread_exit(). A forced unwind through our noexcept event loop frames aborts
// the process; even where it does not, the thread disappears without completing whatever the Python side is
// blocked on. The flag is set from an atexit callback, which runs before CPython arms that behaviour, so a
// reader that sees `false` is guaranteed to be outside the dangerous window.
inline std::atomic<bool>& interpreterIsShuttingDown() noexcept
{
    static std::atomic<bool> shuttingDown {false};
    return shuttingDown;
}

// Acquires the GIL, unless the interpreter is shutting down, in which case acquiring it would kill the calling
// thread. Callers must check `acquired()` before touching any Python state.
class AcquireGIL {
public:
    AcquireGIL(): _acquired {!interpreterIsShuttingDown().load(std::memory_order_acquire)}
    {
        if (_acquired)
            _state = PyGILState_Ensure();
    }
    ~AcquireGIL()
    {
        if (_acquired)
            PyGILState_Release(_state);
    }

    AcquireGIL(const AcquireGIL&)            = delete;
    AcquireGIL& operator=(const AcquireGIL&) = delete;
    AcquireGIL(AcquireGIL&&)                 = delete;
    AcquireGIL& operator=(AcquireGIL&&)      = delete;

    bool acquired() const noexcept
    {
        return _acquired;
    }

private:
    bool _acquired;
    PyGILState_STATE _state {};
};

}  // namespace pymod
}  // namespace utility
}  // namespace scaler

#include <windows.h>

#include <cstdint>

#include "scaler/utility/error.h"

namespace scaler {
namespace utility {
namespace pipe {

std::pair<int64_t, int64_t> createPipe()
{
    SECURITY_ATTRIBUTES sa {};
    sa.nLength        = sizeof(sa);
    sa.bInheritHandle = TRUE;

    HANDLE reader = INVALID_HANDLE_VALUE;
    HANDLE writer = INVALID_HANDLE_VALUE;

    if (!CreatePipe(&reader, &writer, &sa, 0)) {
        unrecoverableError({
            Error::ErrorCode::CoreBug,
            "Originated from",
            "CreatePipe()",
        });
    }

    return std::make_pair((int64_t)reader, (int64_t)writer);
}

void setNonBlocking(int64_t handle)
{
    HANDLE h   = (HANDLE)handle;
    DWORD mode = PIPE_READMODE_BYTE | PIPE_NOWAIT;
    if (!SetNamedPipeHandleState(h, &mode, nullptr, nullptr)) {
        unrecoverableError({
            Error::ErrorCode::CoreBug,
            "Originated from",
            "SetNamedPipeHandleState()",
        });
    }
}

}  // namespace pipe
}  // namespace utility
}  // namespace scaler

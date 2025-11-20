#include <windows.h>

#include <cstdint>

#include "scaler/utility/error.h"
#include "scaler/utility/pipe/pipe.h"

namespace scaler {
namespace utility {
namespace pipe {

std::pair<int64_t, int64_t> create_pipe()
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

}  // namespace pipe
}  // namespace utility
}  // namespace scaler

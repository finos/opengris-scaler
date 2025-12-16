#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>

#include "scaler/utility/error.h"

namespace scaler {
namespace utility {
namespace pipe {

std::pair<int64_t, int64_t> createPipe()
{
    int fds[2] {};
    if (::pipe(fds) < 0) {
        unrecoverableError({
            Error::ErrorCode::CoreBug,
            "Originated from",
            "pipe(2)",
            "Errno is",
            strerror(errno),
        });
    }

    return std::make_pair(fds[0], fds[1]);
}

void setNonBlocking(int64_t handle)
{
    int flags = ::fcntl(handle, F_GETFL, 0);
    if (flags < 0 || ::fcntl(handle, F_SETFL, flags | O_NONBLOCK) < 0) {
        unrecoverableError({
            Error::ErrorCode::CoreBug,
            "Originated from",
            "fcntl(2)",
            "Errno is",
            strerror(errno),
        });
    }
}

}  // namespace pipe
}  // namespace utility
}  // namespace scaler

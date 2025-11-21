#include <unistd.h>

#include <cerrno>

#include "scaler/utility/error.h"
#include "scaler/utility/pipe/pipe.h"

namespace scaler {
namespace utility {
namespace pipe {

std::pair<long long, long long> create_pipe()
{
    int fds[2];
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

}  // namespace pipe
}  // namespace utility
}  // namespace scaler

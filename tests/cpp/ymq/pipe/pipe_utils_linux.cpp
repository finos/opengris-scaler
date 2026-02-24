#include <unistd.h>

#include "tests/cpp/ymq/common/utils.h"
#include "tests/cpp/ymq/pipe/pipe.h"

std::pair<long long, long long> createPipe()
{
    int fds[2];
    if (::pipe(fds) < 0)
        raiseSystemError("pipe");

    return std::make_pair(fds[0], fds[1]);
}

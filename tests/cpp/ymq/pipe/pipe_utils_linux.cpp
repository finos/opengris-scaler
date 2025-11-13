#include <unistd.h>

#include "tests/cpp/ymq/common/utils.h"
#include "tests/cpp/ymq/pipe/pipe.h"

std::pair<long long, long long> create_pipe()
{
    int fds[2];
    if (::pipe(fds) < 0)
        raise_system_error("pipe");

    return std::make_pair(fds[0], fds[1]);
}

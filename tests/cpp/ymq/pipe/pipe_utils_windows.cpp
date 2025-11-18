#include <windows.h>

#include "tests/cpp/ymq/common/utils.h"
#include "tests/cpp/ymq/pipe/pipe.h"

std::pair<long long, long long> create_pipe()
{
    SECURITY_ATTRIBUTES sa {};
    sa.nLength        = sizeof(sa);
    sa.bInheritHandle = TRUE;

    HANDLE reader = INVALID_HANDLE_VALUE;
    HANDLE writer = INVALID_HANDLE_VALUE;

    if (!CreatePipe(&reader, &writer, &sa, 0))
        raise_system_error("failed to create pipe");

    return std::make_pair((long long)reader, (long long)writer);
}

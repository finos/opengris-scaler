#include <errno.h>

#include <system_error>

void raiseSystemError(const char* msg)
{
    throw std::system_error(errno, std::generic_category(), msg);
}

void raiseSocketError(const char* msg)
{
    throw std::system_error(errno, std::generic_category(), msg);
}

#include <errno.h>

#include <system_error>

void raise_system_error(const char* msg)
{
    throw std::system_error(errno, std::generic_category(), msg);
}

void raise_socket_error(const char* msg)
{
    throw std::system_error(errno, std::generic_category(), msg);
}

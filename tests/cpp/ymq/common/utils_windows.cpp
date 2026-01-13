#include <Windows.h>
#include <winsock2.h>

#include <system_error>

void raise_system_error(const char* msg)
{
    throw std::system_error(GetLastError(), std::generic_category(), msg);
}

void raise_socket_error(const char* msg)
{
    throw std::system_error(WSAGetLastError(), std::generic_category(), msg);
}
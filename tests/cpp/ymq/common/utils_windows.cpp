#include <Windows.h>
#include <winsock2.h>

#include <system_error>

void raiseSystemError(const char* msg)
{
    throw std::system_error(GetLastError(), std::generic_category(), msg);
}

void raiseSocketError(const char* msg)
{
    throw std::system_error(WSAGetLastError(), std::generic_category(), msg);
}
#include <Windows.h>
#include <winsock2.h>
#include <ws2tcpip.h>

#include <system_error>

void raiseSystemError(const char* msg)
{
    throw std::system_error(GetLastError(), std::generic_category(), msg);
}

void raiseSocketError(const char* msg)
{
    throw std::system_error(WSAGetLastError(), std::generic_category(), msg);
}

int getFreePort()
{
    // WSAStartup is reference-counted, so this is safe even if Winsock is already initialised by the
    // socket tests; the matching WSACleanup below only decrements that count.
    WSADATA wsaData;
    bool initialised = WSAStartup(MAKEWORD(2, 2), &wsaData) == 0;

    SOCKET fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd == INVALID_SOCKET) {
        if (initialised) {
            WSACleanup();
        }
        raiseSocketError("getFreePort: socket");
    }

    sockaddr_in addr {};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port        = 0;  // let the OS assign a free ephemeral port

    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        ::closesocket(fd);
        if (initialised) {
            WSACleanup();
        }
        raiseSocketError("getFreePort: bind");
    }

    int len = sizeof(addr);
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) != 0) {
        ::closesocket(fd);
        if (initialised) {
            WSACleanup();
        }
        raiseSocketError("getFreePort: getsockname");
    }

    int port = ntohs(addr.sin_port);
    ::closesocket(fd);
    if (initialised) {
        WSACleanup();
    }
    return port;
}
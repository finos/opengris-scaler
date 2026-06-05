#include <winsock2.h>
#include <ws2tcpip.h>

#include <stdexcept>

#include "scaler/object_storage/io_helper.h"

namespace scaler {
namespace object_storage {

int getAvailableTCPPort()
{
    struct WSAStartupGuard {
        WSAStartupGuard()
        {
            WSADATA data {};
            if (WSAStartup(MAKEWORD(2, 2), &data) != 0) {
                throw std::runtime_error("WSAStartup() failed");
            }
        }

        ~WSAStartupGuard()
        {
            WSACleanup();
        }
    } wsaStartupGuard;
    (void)wsaStartupGuard;

    SOCKET socketFileDescriptor = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (socketFileDescriptor == INVALID_SOCKET) {
        return -1;
    }

    sockaddr_in address {};
    address.sin_family      = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port        = 0;

    if (bind(socketFileDescriptor, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == SOCKET_ERROR) {
        closesocket(socketFileDescriptor);
        throw std::runtime_error("bind() failed");
    }

    int addressLength = sizeof(address);
    if (getsockname(socketFileDescriptor, reinterpret_cast<sockaddr*>(&address), &addressLength) == SOCKET_ERROR) {
        closesocket(socketFileDescriptor);
        throw std::runtime_error("getsockname() failed");
    }

    const int port = ntohs(address.sin_port);

    closesocket(socketFileDescriptor);

    return port;
}

};  // namespace object_storage
};  // namespace scaler

#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <stdexcept>

#include "scaler/object_storage/io_helper.h"

namespace scaler {
namespace object_storage {

int getAvailableTCPPort()
{
    int socketFileDescriptor = socket(AF_INET, SOCK_STREAM, 0);
    if (socketFileDescriptor < 0) {
        return -1;
    }

    sockaddr_in address {};
    address.sin_family      = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port        = 0;

    if (bind(socketFileDescriptor, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0) {
        close(socketFileDescriptor);
        throw std::runtime_error("bind() failed");
    }

    socklen_t addressLength = sizeof(address);
    if (getsockname(socketFileDescriptor, reinterpret_cast<sockaddr*>(&address), &addressLength) < 0) {
        close(socketFileDescriptor);
        throw std::runtime_error("getsockname() failed");
    }

    const int port = ntohs(address.sin_port);

    close(socketFileDescriptor);

    return port;
}

};  // namespace object_storage
};  // namespace scaler

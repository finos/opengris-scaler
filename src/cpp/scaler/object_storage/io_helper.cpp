#include "scaler/object_storage/io_helper.h"

#include <netinet/in.h>  // sockaddr_in
#include <sys/socket.h>  // socket(2)
#include <unistd.h>      // close(2)

#include <exception>
#include <iostream>

namespace scaler {
namespace object_storage {

int getAvailableTCPPort()
{
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        return -1;
    }

    sockaddr_in addr {};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port        = 0;  // Let OS choose port

    if (bind(sockfd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        close(sockfd);
        throw std::runtime_error("bind() failed");
    }

    socklen_t len = sizeof(addr);
    if (getsockname(sockfd, reinterpret_cast<sockaddr*>(&addr), &len) < 0) {
        close(sockfd);
        throw std::runtime_error("getsockname() failed");
    }

    const int port = ntohs(addr.sin_port);

    close(sockfd);

    return port;
}

};  // namespace object_storage
};  // namespace scaler

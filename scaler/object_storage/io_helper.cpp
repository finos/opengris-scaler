#include "io_helper.h"

#include <iostream>

using boost::asio::ip::tcp;

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
        return -1;
    }

    socklen_t len = sizeof(addr);
    if (getsockname(sockfd, reinterpret_cast<sockaddr*>(&addr), &len) < 0) {
        close(sockfd);
        return -1;
    }

    const int port = ntohs(addr.sin_port);

    close(sockfd);

    return port;
}

void setTCPNoDelay(tcp::socket& socket, bool isNoDelay)
{
    boost::system::error_code ec;
    socket.set_option(tcp::no_delay(isNoDelay), ec);

    if (ec) {
        std::cerr << "failed to set TCP_NODELAY on client socket: " << ec.message() << std::endl;
        exit(1);
    }
}

};  // namespace object_storage
};  // namespace scaler

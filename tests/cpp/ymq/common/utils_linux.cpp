#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <system_error>

void raiseSystemError(const char* msg)
{
    throw std::system_error(errno, std::generic_category(), msg);
}

void raiseSocketError(const char* msg)
{
    throw std::system_error(errno, std::generic_category(), msg);
}

int getFreePort()
{
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        raiseSocketError("getFreePort: socket");
    }

    sockaddr_in addr {};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port        = 0;  // let the OS assign a free ephemeral port

    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        ::close(fd);
        raiseSocketError("getFreePort: bind");
    }

    socklen_t len = sizeof(addr);
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) != 0) {
        ::close(fd);
        raiseSocketError("getFreePort: getsockname");
    }

    int port = ntohs(addr.sin_port);
    ::close(fd);
    return port;
}

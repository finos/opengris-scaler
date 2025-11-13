#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <thread>
#include <vector>

#include "tests/cpp/ymq/common/utils.h"
#include "tests/cpp/ymq/net/socket.h"

Socket::Socket(bool nodelay): _fd(-1), _nodelay(nodelay)
{
    this->_fd = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (this->_fd < 0)
        raise_socket_error("failed to create socket");

    char on = 1;
    if (this->_nodelay)
        if (::setsockopt(this->_fd, IPPROTO_TCP, TCP_NODELAY, (const char*)&on, sizeof(on)) < 0)
            raise_socket_error("failed to set nodelay");
}

Socket::Socket(bool nodelay, long long fd): _fd(fd), _nodelay(nodelay)
{
    char on = 1;
    if (this->_nodelay)
        if (::setsockopt(this->_fd, IPPROTO_TCP, TCP_NODELAY, (const char*)&on, sizeof(on)) < 0)
            raise_socket_error("failed to set nodelay");
}

Socket::~Socket()
{
    close(this->_fd);
}

Socket::Socket(Socket&& other) noexcept
{
    this->_nodelay = other._nodelay;
    this->_fd      = other._fd;
    other._fd      = -1;
}

Socket& Socket::operator=(Socket&& other) noexcept
{
    this->_nodelay = other._nodelay;
    this->_fd      = other._fd;
    other._fd      = -1;
    return *this;
}

void Socket::try_connect(const std::string& host, short port, int tries) const
{
    sockaddr_in addr {};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(port);
    inet_pton(AF_INET, check_localhost(host.c_str()), &addr.sin_addr);

    for (int i = 0; i < tries; i++) {
        auto code = ::connect(this->_fd, (sockaddr*)&addr, sizeof(addr));

        if (code < 0) {
            if (errno == ECONNREFUSED) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                continue;
            }

            raise_socket_error("failed to connect");
        }

        break;  // success
    }
}

void Socket::bind(short port) const
{
    sockaddr_in addr {};
    addr.sin_family      = AF_INET;
    addr.sin_port        = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;
    if (::bind(this->_fd, (sockaddr*)&addr, sizeof(addr)) < 0)
        raise_socket_error("failed to bind");
}

void Socket::listen(int backlog) const
{
    if (::listen(this->_fd, backlog) < 0)
        raise_socket_error("failed to listen");
}

Socket Socket::accept() const
{
    long long fd = ::accept(this->_fd, nullptr, nullptr);
    if (fd < 0)
        raise_socket_error("failed to accept");

    return Socket(this->_nodelay, fd);
}

int Socket::write(const void* buffer, size_t size) const
{
    int n = ::write(this->_fd, buffer, size);
    if (n < 0)
        raise_socket_error("failed to send");
    return n;
}

void Socket::write_all(const void* buffer, size_t size) const
{
    size_t cursor = 0;
    while (cursor < size)
        cursor += (size_t)this->write((char*)buffer + cursor, size - cursor);
}

void Socket::write_all(std::string msg) const
{
    this->write_all(msg.data(), msg.size());
}

void Socket::write_message(std::string msg) const
{
    uint64_t header = msg.length();
    this->write_all(&header, 8);
    this->write_all(msg.data(), msg.length());
}

int Socket::read(void* buffer, size_t size) const
{
    int n = ::read(this->_fd, buffer, size);
    if (n < 0)
        raise_socket_error("failed to recv");
    return n;
}

void Socket::read_exact(void* buffer, size_t size) const
{
    size_t cursor = 0;
    while (cursor < size)
        cursor += (size_t)this->read((char*)buffer + cursor, size - cursor);
}

std::string Socket::read_message() const
{
    uint64_t header = 0;
    this->read_exact(&header, 8);
    std::vector<char> buffer(header);
    this->read_exact(buffer.data(), header);
    return std::string(buffer.data(), header);
}

#include <Windows.h>
#include <winsock2.h>
#include <ws2tcpip.h>

#include <memory>
#include <optional>
#include <stdexcept>
#include <thread>
#include <vector>

#include "scaler/ymq/internal/network_utils.h"
#include "scaler/ymq/internal/socket_address.h"
#include "tests/cpp/ymq/common/utils.h"
#include "tests/cpp/ymq/net/tcp_socket.h"

using scaler::ymq::SocketAddress;

TCPSocket::TCPSocket(bool nodelay): _fd(-1), _nodelay(nodelay)
{
    this->_fd = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (this->_fd == SOCKET_ERROR)
        raise_socket_error("failed to create socket");

    char on = 1;
    if (this->_nodelay)
        if (::setsockopt((SOCKET)this->_fd, IPPROTO_TCP, TCP_NODELAY, (const char*)&on, sizeof(on)) == SOCKET_ERROR)
            raise_socket_error("failed to set nodelay");
}

TCPSocket::TCPSocket(bool nodelay, long long fd): _fd(fd), _nodelay(nodelay)
{
    char on = 1;
    if (this->_nodelay)
        if (::setsockopt((SOCKET)this->_fd, IPPROTO_TCP, TCP_NODELAY, (const char*)&on, sizeof(on)) == SOCKET_ERROR)
            raise_socket_error("failed to set nodelay");
}

TCPSocket::~TCPSocket()
{
    ::closesocket((SOCKET)this->_fd);
}

TCPSocket::TCPSocket(TCPSocket&& other) noexcept
{
    this->_nodelay = other._nodelay;
    this->_fd      = other._fd;
    other._fd      = -1;
}

TCPSocket& TCPSocket::operator=(TCPSocket&& other) noexcept
{
    this->_nodelay = other._nodelay;
    this->_fd      = other._fd;
    other._fd      = -1;
    return *this;
}

void TCPSocket::tryConnect(const std::string& address_str, int tries) const
{
    auto address = scaler::ymq::stringToSocketAddress(address_str);
    if (address.nativeHandleType() != SocketAddress::Type::TCP) {
        throw std::runtime_error(std::format("Unsupported protocol for TCPSocket: {}", address.nativeHandleType()));
    }

    sockaddr_in addr = *(sockaddr_in*)address.nativeHandle();

    for (int i = 0; i < tries; i++) {
        auto code = ::connect((SOCKET)this->_fd, (sockaddr*)&addr, sizeof(addr));

        if (code == SOCKET_ERROR) {
            if (WSAGetLastError() == WSAECONNREFUSED) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                continue;
            }

            raise_socket_error("failed to connect");
        }

        break;  // success
    }
}

void TCPSocket::bind(const std::string& address_str) const
{
    auto address = scaler::ymq::stringToSocketAddress(address_str);
    if (address.nativeHandleType() != SocketAddress::Type::TCP) {
        throw std::runtime_error(std::format("Unsupported protocol for TCPSocket: {}", address.nativeHandleType()));
    }

    sockaddr_in addr = *(sockaddr_in*)address.nativeHandle();

    if (::bind((SOCKET)this->_fd, (sockaddr*)&addr, sizeof(addr)) == SOCKET_ERROR)
        raise_socket_error("failed to bind");
}

void TCPSocket::listen(int backlog) const
{
    if (::listen((SOCKET)this->_fd, backlog) == SOCKET_ERROR)
        raise_socket_error("failed to listen");
}

std::unique_ptr<Socket> TCPSocket::accept() const
{
    long long fd = ::accept((SOCKET)this->_fd, nullptr, nullptr);
    if (fd == SOCKET_ERROR)
        raise_socket_error("failed to accept");

    return std::make_unique<TCPSocket>(this->_nodelay, fd);
}

int TCPSocket::write(const void* buffer, size_t size) const
{
    auto n = ::send((SOCKET)this->_fd, static_cast<const char*>(buffer), (int)size, 0);
    if (n == SOCKET_ERROR)
        raise_socket_error("failed to send data");
    return n;
}

void TCPSocket::writeAll(const void* buffer, size_t size) const
{
    size_t cursor = 0;
    while (cursor < size)
        cursor += (size_t)this->write((char*)buffer + cursor, size - cursor);
}

void TCPSocket::writeAll(std::string msg) const
{
    this->writeAll(msg.data(), msg.size());
}

void TCPSocket::writeMessage(std::string msg) const
{
    uint64_t header = msg.length();
    this->writeAll(&header, 8);
    this->writeAll(msg.data(), msg.length());
}

int TCPSocket::read(void* buffer, size_t size) const
{
    auto n = ::recv((SOCKET)this->_fd, static_cast<char*>(buffer), (int)size, 0);
    if (n == SOCKET_ERROR)
        raise_socket_error("failed to receive data");
    return n;
}

void TCPSocket::readExact(void* buffer, size_t size) const
{
    size_t cursor = 0;
    while (cursor < size)
        cursor += (size_t)this->read((char*)buffer + cursor, size - cursor);
}

std::string TCPSocket::readMessage() const
{
    uint64_t header = 0;
    this->readExact(&header, 8);
    std::vector<char> buffer(header);
    this->readExact(buffer.data(), header);
    return std::string(buffer.data(), header);
}

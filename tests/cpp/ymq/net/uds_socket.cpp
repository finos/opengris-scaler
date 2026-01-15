#include "tests/cpp/ymq/net/uds_socket.h"

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <cerrno>
#include <stdexcept>
#include <thread>
#include <vector>

#include "scaler/ymq/internal/network_utils.h"
#include "scaler/ymq/internal/socket_address.h"
#include "tests/cpp/ymq/common/utils.h"

using scaler::ymq::SocketAddress;

UDSSocket::UDSSocket(): _fd(-1)
{
    this->_fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (this->_fd < 0)
        raise_socket_error("failed to create socket");
}

UDSSocket::UDSSocket(long long fd): _fd(fd)
{
}

UDSSocket::~UDSSocket()
{
    close(this->_fd);
}

UDSSocket::UDSSocket(UDSSocket&& other) noexcept
{
    this->_fd = other._fd;
    other._fd = -1;
}

UDSSocket& UDSSocket::operator=(UDSSocket&& other) noexcept
{
    this->_fd = other._fd;
    other._fd = -1;
    return *this;
}

void UDSSocket::tryConnect(const std::string& address_str, int tries) const
{
    auto address = scaler::ymq::stringToSocketAddress(address_str);
    if (address.nativeHandleType() != SocketAddress::Type::IPC) {
        throw std::runtime_error(std::format("Unsupported protocol for UDSSocket: {}", address.nativeHandleType()));
    }

    sockaddr_un addr = *(sockaddr_un*)address.nativeHandle();

    for (int i = 0; i < tries; i++) {
        auto code = ::connect(this->_fd, (sockaddr*)&addr, sizeof(addr));

        if (code < 0) {
            if (errno == ENOENT || errno == ECONNREFUSED) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                continue;
            }

            raise_socket_error("failed to connect");
        }

        break;  // success
    }
}

void UDSSocket::bind(const std::string& address_str) const
{
    auto address = scaler::ymq::stringToSocketAddress(address_str);
    if (address.nativeHandleType() != SocketAddress::Type::IPC) {
        throw std::runtime_error(std::format("Unsupported protocol for UDSSocket: {}", address.nativeHandleType()));
    }

    sockaddr_un addr = *(sockaddr_un*)address.nativeHandle();

    ::unlink(addr.sun_path);
    if (::bind(this->_fd, (sockaddr*)&addr, sizeof(addr)) < 0)
        raise_socket_error("failed to bind");
}

void UDSSocket::listen(int backlog) const
{
    if (::listen(this->_fd, backlog) < 0)
        raise_socket_error("failed to listen");
}

std::unique_ptr<Socket> UDSSocket::accept() const
{
    long long fd = ::accept(this->_fd, nullptr, nullptr);
    if (fd < 0)
        raise_socket_error("failed to accept");

    return std::make_unique<UDSSocket>(fd);
}

int UDSSocket::write(const void* buffer, size_t size) const
{
    int n = ::write(this->_fd, buffer, size);
    if (n < 0)
        raise_socket_error("failed to send");
    return n;
}

void UDSSocket::writeAll(const void* buffer, size_t size) const
{
    size_t cursor = 0;
    while (cursor < size)
        cursor += (size_t)this->write((char*)buffer + cursor, size - cursor);
}

void UDSSocket::writeAll(std::string msg) const
{
    this->writeAll(msg.data(), msg.size());
}

void UDSSocket::writeMessage(std::string msg) const
{
    uint64_t header = msg.length();
    this->writeAll(&header, 8);
    this->writeAll(msg.data(), msg.length());
}

int UDSSocket::read(void* buffer, size_t size) const
{
    int n = ::read(this->_fd, buffer, size);
    if (n < 0)
        raise_socket_error("failed to recv");
    return n;
}

void UDSSocket::readExact(void* buffer, size_t size) const
{
    size_t cursor = 0;
    while (cursor < size)
        cursor += (size_t)this->read((char*)buffer + cursor, size - cursor);
}

std::string UDSSocket::readMessage() const
{
    uint64_t header = 0;
    this->readExact(&header, 8);
    std::vector<char> buffer(header);
    this->readExact(buffer.data(), header);
    return std::string(buffer.data(), header);
}

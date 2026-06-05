#include "tests/cpp/ymq/net/uds_socket.h"

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <thread>
#include <vector>

#include "scaler/ymq/address.h"
#include "tests/cpp/ymq/common/utils.h"

static sockaddr_un createUnixAddress(const scaler::ymq::Address& address)
{
    if (address.type() != scaler::ymq::Address::Type::IPC) {
        throw std::runtime_error("Unsupported protocol for UDSSocket: expected IPC");
    }

    const std::string& path = address.asIPC();
    sockaddr_un addr {};
    addr.sun_family = AF_UNIX;
    std::strncpy(addr.sun_path, path.c_str(), sizeof(addr.sun_path) - 1);
    return addr;
}

UDSSocket::UDSSocket(): _fd(-1)
{
    this->_fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (this->_fd < 0)
        raiseSocketError("failed to create socket");
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

void UDSSocket::tryConnect(const scaler::ymq::Address& address, int tries) const
{
    sockaddr_un addr = createUnixAddress(address);

    for (int i = 0; i < tries; i++) {
        auto code = ::connect(this->_fd, (sockaddr*)&addr, sizeof(addr));

        if (code < 0) {
            if (errno == ENOENT || errno == ECONNREFUSED) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                continue;
            }

            raiseSocketError("failed to connect");
        }

        break;  // success
    }
}

void UDSSocket::bind(const scaler::ymq::Address& address) const
{
    sockaddr_un addr = createUnixAddress(address);

    ::unlink(addr.sun_path);
    if (::bind(this->_fd, (sockaddr*)&addr, sizeof(addr)) < 0)
        raiseSocketError("failed to bind");
}

void UDSSocket::listen(int backlog) const
{
    if (::listen(this->_fd, backlog) < 0)
        raiseSocketError("failed to listen");
}

std::unique_ptr<Socket> UDSSocket::accept() const
{
    long long fd = ::accept(this->_fd, nullptr, nullptr);
    if (fd < 0)
        raiseSocketError("failed to accept");

    return std::make_unique<UDSSocket>(fd);
}

int UDSSocket::write(const void* buffer, size_t size) const
{
    int n = ::write(this->_fd, buffer, size);
    if (n < 0)
        raiseSocketError("failed to send");
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
        raiseSocketError("failed to recv");
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

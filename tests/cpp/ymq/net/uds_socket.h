#pragma once
#include <memory>
#include <string>

#include "socket.h"

class UDSSocket: public Socket {
public:
    UDSSocket();
    UDSSocket(long long fd);
    ~UDSSocket();

    // move-only
    UDSSocket(UDSSocket&&) noexcept;
    UDSSocket& operator=(UDSSocket&&) noexcept;
    UDSSocket(const UDSSocket&)            = delete;
    UDSSocket& operator=(const UDSSocket&) = delete;

    void tryConnect(const std::string& address, int tries = 10) const override;
    void bind(const std::string& address) const override;
    void listen(int backlog = 5) const override;
    std::unique_ptr<Socket> accept() const override;

    void writeAll(const void* data, size_t size) const override;
    void writeAll(std::string msg) const override;

    void readExact(void* buffer, size_t size) const override;

    void writeMessage(std::string msg) const override;

    std::string readMessage() const override;

private:
    long long _fd;

    // write up to `size` bytes
    int write(const void* buffer, size_t size) const;

    // read up to `size` bytes
    int read(void* buffer, size_t size) const;
};

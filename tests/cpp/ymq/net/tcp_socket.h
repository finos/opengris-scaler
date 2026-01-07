#pragma once
#include <memory>
#include <string>

#include "socket.h"

class TCPSocket: public Socket {
public:
    TCPSocket(bool nodelay = false);
    TCPSocket(bool nodelay, long long fd);
    ~TCPSocket();

    // move-only
    TCPSocket(TCPSocket&&) noexcept;
    TCPSocket& operator=(TCPSocket&&) noexcept;
    TCPSocket(const TCPSocket&)            = delete;
    TCPSocket& operator=(const TCPSocket&) = delete;

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
    // the native handle for this pipe reader
    // on Linux, this is a file descriptor
    // on Windows, this is a SOCKET
    long long _fd;

    // indicates if nodelay was set
    bool _nodelay;

    // write up to `size` bytes
    int write(const void* buffer, size_t size) const;

    // read up to `size` bytes
    int read(void* buffer, size_t size) const;
};

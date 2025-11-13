#pragma once
#include <cstdint>
#include <memory>
#include <optional>
#include <string>

class Socket {
public:
    Socket(bool nodelay = false);
    Socket(bool nodelay, long long fd);
    ~Socket();

    // move-only
    Socket(Socket&&) noexcept;
    Socket& operator=(Socket&&) noexcept;
    Socket(const Socket&)            = delete;
    Socket& operator=(const Socket&) = delete;

    // try to connect, retrying up to `tries` times when the connection is refused
    void try_connect(const std::string& host, short port, int tries = 10) const;
    void bind(short port) const;
    void listen(int backlog = 5) const;
    Socket accept() const;

    // write an entire buffer
    void write_all(const void* data, size_t size) const;
    void write_all(std::string msg) const;

    // read exactly `size` bytes
    void read_exact(void* buffer, size_t size) const;

    // write a message in the YMQ protocol
    void write_message(std::string msg) const;

    // read a YMQ message
    std::string read_message() const;

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

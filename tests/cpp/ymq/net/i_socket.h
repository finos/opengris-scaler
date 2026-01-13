#pragma once
#include <cstdint>
#include <memory>
#include <optional>
#include <string>

class ISocket {
public:
    virtual ~ISocket() = default;

    virtual void try_connect(const std::string& address, int tries = 10) const = 0;
    virtual void bind(const std::string& address) const                        = 0;
    virtual void listen(int backlog = 5) const                                 = 0;
    virtual std::unique_ptr<ISocket> accept() const                            = 0;

    virtual void write_all(const void* data, size_t size) const = 0;
    virtual void write_all(std::string msg) const               = 0;

    virtual void read_exact(void* buffer, size_t size) const = 0;

    virtual void write_message(std::string msg) const = 0;

    virtual std::string read_message() const = 0;
};

#pragma once

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <functional>
#include <future>
#include <print>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

inline void exception_handler(std::function<void()> fn)
{
    try {
        fn();
    } catch (const std::exception& e) {
        std::println("Exception: {}", e.what());
        throw;
    }
}

inline void harness(std::string test_description, int timeout_secs, std::function<void()> test)
{
    std::println("Running: {}", test_description);

    auto future = std::async(std::launch::async, test);
    auto result = future.wait_for(std::chrono::seconds(timeout_secs));

    switch (result) {
        case std::future_status::ready: std::println("... SUCCESS"); break;
        default: std::println("... FAILED"); std::exit(1);
    }
}

class TcpSocket {
    int _socket;

public:
    TcpSocket()
    {
        this->_socket = ::socket(AF_INET, SOCK_STREAM, 0);
        if (this->_socket < 0)
            throw std::runtime_error("failed to create socket");

        int on = 1;
        if (setsockopt(this->_socket, IPPROTO_TCP, TCP_NODELAY, (char*)&on, sizeof(on)) < 0)
            throw std::runtime_error("failed to set TCP_NODELAY");
    }

    ~TcpSocket() { close(this->_socket); }

    void connect(const char* sAddr, int port)
    {
        sockaddr_in addr {
            .sin_family = AF_INET, .sin_port = htons(25711), .sin_addr = {.s_addr = inet_addr(sAddr)}, .sin_zero = {0}};

    connect:
        if (::connect(this->_socket, (sockaddr*)&addr, sizeof(addr)) < 0) {
            if (errno == ECONNREFUSED) {
                std::this_thread::sleep_for(300ms);
                goto connect;
            }

            throw std::runtime_error("failed to connect");
        }
    }

    size_t write(const void* data, size_t len)
    {
        auto n = ::write(this->_socket, data, len);
        if (n < 0)
            throw std::runtime_error("failed to write to socket");

        return n;
    }

    void write_all(const char* data, size_t len)
    {
        size_t cursor = 0;
        while (cursor < len) {
            cursor += this->write(data + cursor, len - cursor);
        }
    }

    size_t read(void* buffer, size_t len)
    {
        auto n = ::read(this->_socket, buffer, len);
        if (n < 0)
            throw std::runtime_error("failed to read from socket");
        return n;
    }

    void read_exact(char* buffer, size_t len)
    {
        size_t cursor = 0;
        while (cursor < len) {
            cursor += this->read(buffer + cursor, len - cursor);
        }
    }

    void write_message(std::string message)
    {
        uint64_t header = message.length();
        this->write_all((char*)&header, 8);
        this->write_all(message.data(), message.length());
    }

    std::string read_message()
    {
        uint64_t header = 0;
        this->read_exact((char*)&header, 8);
        std::vector<char> buffer(header);
        this->read_exact(buffer.data(), header);
        return std::string(buffer.data(), header);
    }
};

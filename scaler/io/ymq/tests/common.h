#pragma once

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <csignal>
#include <cstdio>
#include <exception>
#include <functional>
#include <future>
#include <print>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

enum class TestResult { Success, Failure };

struct Counters {
    int successes = 0;
    int failures  = 0;

    int total() const { return this->successes + this->failures; }

    Counters& operator+=(const TestResult& other)
    {
        switch (other) {
            case TestResult::Success: this->successes++; break;
            case TestResult::Failure: this->failures++; break;
        }

        return *this;
    }
};

inline void thread_wrapper(std::function<void()> fn, int timeout_secs, std::promise<TestResult>& promise)
{
    try {
        auto future = std::async(std::launch::async, fn);
        auto result = future.wait_for(std::chrono::seconds(timeout_secs));

        switch (result) {
            case std::future_status::ready: promise.set_value(TestResult::Success); break;
            default: promise.set_value(TestResult::Failure);
        }
    } catch (const std::exception& e) {
        std::println("Exception: {}", e.what());
        promise.set_value(TestResult::Failure);
        throw;
    }
}

inline TestResult test(int timeout_secs, std::function<void()> client_main, std::function<void()> server_main)
{
    auto server_promise = std::promise<TestResult>();
    std::thread server([&] { thread_wrapper(server_main, timeout_secs, server_promise); });
    auto client_promise = std::promise<TestResult>();
    std::thread client([&] { thread_wrapper(client_main, timeout_secs, client_promise); });

    // guaranteed that these will exit in <=timeout_secs because of `std::future::wait_for()`
    server.detach();
    client.detach();

    if (server_promise.get_future().get() != TestResult::Success)
        return TestResult::Failure;

    if (client_promise.get_future().get() != TestResult::Success)
        return TestResult::Failure;

    return TestResult::Success;
}

inline TestResult test(
    std::string test_description,
    int timeout_secs,
    std::function<void()> client_main,
    std::function<void()> server_main)
{
    std::print("Running: {} ... ", test_description);
    std::fflush(stdout);

    auto result = test(timeout_secs, client_main, server_main);

    switch (result) {
        case TestResult::Success: std::println("SUCCESS"); break;
        case TestResult::Failure: std::println("FAILED");
    }

    return result;
}

static void handler(int signo)
{
    std::println("Received signal: {}", signo);
}

inline void setup_signal_handlers()
{
    int signals[] = {SIGPIPE};

    struct sigaction action {0};
    action.sa_handler = handler;
    sigemptyset(&action.sa_mask);
    action.sa_flags = 0;
    for (auto signo: signals) {
        if (sigaction(signo, &action, nullptr) < 0)
            throw std::runtime_error("failed to set signal handler");
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
            .sin_family = AF_INET, .sin_port = htons(port), .sin_addr = {.s_addr = inet_addr(sAddr)}, .sin_zero = {0}};

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

#pragma once

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/poll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/timerfd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <optional>
#include <print>
#include <stdexcept>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

#define ASSERT(condition)           \
    if (!(condition)) {             \
        return TestResult::Failure; \
    }

using namespace std::chrono_literals;

enum class TestResult : char { Success = 1, Failure = 2 };

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

class OwnedFd {
protected:
    int _fd;

public:
    OwnedFd(int fd): _fd(fd) {}

    // move-only
    OwnedFd(const OwnedFd&)            = delete;
    OwnedFd& operator=(const OwnedFd&) = delete;
    OwnedFd(OwnedFd&& other) noexcept: _fd(other._fd) { other._fd = 0; }
    OwnedFd& operator=(OwnedFd&& other) noexcept
    {
        if (this != &other) {
            this->_fd = other._fd;
            other._fd = 0;
        }
        return *this;
    }

    ~OwnedFd()
    {
        if (close(_fd) < 0)
            std::println(std::cerr, "failed to close fd!");
    }

    size_t write(const void* data, size_t len)
    {
        auto n = ::write(this->_fd, data, len);
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
        auto n = ::read(this->_fd, buffer, len);
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

    operator int() { return _fd; }
};

inline bool host_is_network_byte_order()
{
    uint16_t x = 0x00FF;

    // if the lowest byte is 0, host is big endian
    return (*((uint8_t*)&x)) == 0x00;
}

struct TcpHeader {
    uint16_t source_port;
    uint16_t destination_port;
    uint32_t seq_num;
    uint32_t ack_num;
    uint8_t data_offset__reserved;
    uint8_t flags;
    uint16_t window;
    uint16_t checksum;
    uint16_t urgent_pointer;
    std::vector<uint8_t> options;

    uint8_t data_offset() { return this->data_offset__reserved & 0b11110000; }
    uint8_t reserved() { return this->data_offset__reserved & 0b00001111; }

    bool cwr() { return (this->flags & 0b10000000) > 0; }
    bool ece() { return (this->flags & 0b01000000) > 0; }
    bool urg() { return (this->flags & 0b00100000) > 0; }
    bool ack() { return (this->flags & 0b00010000) > 0; }
    bool psh() { return (this->flags & 0b00001000) > 0; }
    bool rst() { return (this->flags & 0b00000100) > 0; }
    bool syn() { return (this->flags & 0b00000010) > 0; }
    bool fin() { return (this->flags & 0b00000001) > 0; }

    size_t size() { return this->data_offset() * 4; }

    // ---

    static TcpHeader deserialize(uint8_t* buffer, size_t len)
    {
        auto read_u8 = [](uint8_t* buffer, size_t cursor, uint8_t* x) {
            *x = buffer[cursor];
            return 1;
        };

        auto read_u16 = [](uint8_t* buffer, size_t cursor, uint16_t* x) {
            std::memcpy(x, buffer + cursor, 2);
            *x = ntohs(*x);
            return 2;
        };

        auto read_u32 = [](uint8_t* buffer, size_t cursor, uint32_t* x) {
            std::memcpy(x, buffer + cursor, 4);
            *x = ntohl(*x);
            return 4;
        };

        auto read_n = [](uint8_t* buffer, size_t cursor, uint8_t* data, size_t n) {
            if (host_is_network_byte_order()) {
                std::memcpy(data, buffer + cursor, n);
            } else {
                for (size_t i = 0; i < n; i++)
                    data[i] = buffer[cursor + n - i - 1];
            }
            return n;
        };

        size_t cursor = 0;
        TcpHeader that {};

        cursor += read_u16(buffer, cursor, &that.source_port);
        cursor += read_u16(buffer, cursor, &that.destination_port);
        cursor += read_u32(buffer, cursor, &that.seq_num);
        cursor += read_u32(buffer, cursor, &that.ack_num);
        cursor += read_u8(buffer, cursor, &that.data_offset__reserved);
        cursor += read_u8(buffer, cursor, &that.flags);
        cursor += read_u16(buffer, cursor, &that.window);
        cursor += read_u16(buffer, cursor, &that.checksum);
        cursor += read_u16(buffer, cursor, &that.urgent_pointer);

        for (; cursor < len;) {
            uint8_t kind = 0;
            cursor += read_u8(buffer, cursor, &kind);

            that.options.push_back(kind);

            // end of options
            if (kind == 0) {
                break;
            }

            // no-op
            if (kind == 1) {
                continue;
            }

            uint8_t length = 0;
            cursor += read_u8(buffer, cursor, &length);

            that.options.push_back(length);

            // note: we assume the header is well-formed, so length < 
            uint8_t* data = new uint8_t[length];
            cursor += read_n(buffer, cursor, data, length);
            for (size_t i = 0; i < length; i++)
                that.options.push_back(data[i]);
            delete[] data;
        }

        return that;
    }

    std::unique_ptr<uint8_t[]> serialize()
    {
        auto write_u8 = [](uint8_t* buffer, size_t cursor, uint8_t x) {
            buffer[cursor] = x;
            return 1;
        };

        auto write_u16 = [](uint8_t* buffer, size_t cursor, uint16_t x) {
            x = htons(x);
            std::memcpy(buffer + cursor, &x, 2);
            return 2;
        };

        auto write_u32 = [](uint8_t* buffer, size_t cursor, uint32_t x) {
            x = htonl(x);
            std::memcpy(buffer + cursor, &x, 4);
            return 4;
        };

        auto write_n = [](uint8_t* buffer, size_t cursor, uint8_t* data, size_t n) {
            if (host_is_network_byte_order()) {
                std::memcpy(buffer + cursor, data, n);
            } else {
                // reverse the bytes
                for (size_t i = 0; i < n; i++)
                    buffer[cursor + i] = data[n - i - 1];
            }
            return n;
        };

        size_t cursor   = 0;
        uint8_t* buffer = new uint8_t[this->size()];

        cursor += write_u16(buffer, cursor, this->source_port);
        cursor += write_u16(buffer, cursor, this->destination_port);
        cursor += write_u32(buffer, cursor, this->seq_num);
        cursor += write_u32(buffer, cursor, this->ack_num);
        cursor += write_u8(buffer, cursor, this->data_offset__reserved);
        cursor += write_u8(buffer, cursor, this->flags);
        cursor += write_u16(buffer, cursor, this->window);
        cursor += write_u16(buffer, cursor, this->checksum);
        cursor += write_u16(buffer, cursor, this->urgent_pointer);

        for (size_t i = 0;; i++) {
            auto kind = this->options[i++];
            cursor += write_u8(buffer, cursor, kind);

            if (kind == 0) {
                break;
            }

            if (kind == 1) {
                continue;
            }

            auto length = this->options[i++];
            cursor += write_u8(buffer, cursor, length);
            cursor += write_n(buffer, cursor, this->options.data() + i, length);
            i += length;
        }

        return std::unique_ptr<uint8_t[]>(buffer);
    }
};

class RawSocket: public OwnedFd {
public:
    RawSocket(): OwnedFd(0)
    {
        this->_fd = ::socket(AF_INET, SOCK_RAW, 0);

        if (this->_fd < 0)
            throw std::runtime_error("failed to create socket");
    }
};

class TcpSocket: public OwnedFd {
public:
    TcpSocket(): OwnedFd(0)
    {
        this->_fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (this->_fd < 0)
            throw std::runtime_error("failed to create socket");

        int on = 1;
        if (setsockopt(this->_fd, IPPROTO_TCP, TCP_NODELAY, (char*)&on, sizeof(on)) < 0)
            throw std::runtime_error("failed to set TCP_NODELAY");
    }

    void connect(const char* sAddr, int port)
    {
        sockaddr_in addr {
            .sin_family = AF_INET, .sin_port = htons(port), .sin_addr = {.s_addr = inet_addr(sAddr)}, .sin_zero = {0}};

    connect:
        if (::connect(this->_fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
            if (errno == ECONNREFUSED) {
                std::this_thread::sleep_for(300ms);
                goto connect;
            }

            throw std::runtime_error("failed to connect");
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

inline void fork_wrapper(std::function<TestResult()> fn, int timeout_secs, OwnedFd pipe_wr)
{
    TestResult result = TestResult::Failure;
    try {
        result = fn();
    } catch (const std::exception& e) {
        std::println(std::cerr, "Exception: {}", e.what());
        result = TestResult::Failure;
    }

    pipe_wr.write_all((char*)&result, sizeof(TestResult));
}

// strategy: fork and run the client, then fork again and run the server
// this gives the client and server freedom to run in parallel and should also
// shield us from any potential bugs, e.g. hanging, segmentation faults, etc.
// the processes will communicate back via pipes, which we combine with a timerfd in poll()
// to receive responses from both subprocesses with a timeout
inline TestResult test(
    int timeout_secs, std::function<TestResult()> client_main, std::function<TestResult()> server_main)
{
    int client_pipe[2] = {0};
    if (pipe2(client_pipe, O_NONBLOCK) < 0)
        throw std::runtime_error("failed to create pipe: " + std::to_string(errno));
    auto client_pid = fork();
    if (client_pid == 0) {
        close(client_pipe[0]);
        fork_wrapper(client_main, timeout_secs, client_pipe[1]);
        std::exit(EXIT_SUCCESS);
    }
    close(client_pipe[1]);
    OwnedFd client_rd = client_pipe[0];

    int server_pipe[2] = {0};
    if (pipe2(server_pipe, O_NONBLOCK) < 0)
        throw std::runtime_error("failed to create pipe: " + std::to_string(errno));
    auto server_pid = fork();
    if (server_pid == 0) {
        close(server_pipe[0]);
        fork_wrapper(server_main, timeout_secs, server_pipe[1]);
        std::exit(EXIT_SUCCESS);
    }
    close(server_pipe[1]);
    OwnedFd server_rd = server_pipe[0];

    OwnedFd timerfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
    if (timerfd < 0)
        throw std::runtime_error("failed to create timerfd: " + std::to_string(errno));

    itimerspec spec {
        .it_interval =
            {
                .tv_sec  = 0,
                .tv_nsec = 0,
            },
        .it_value = {
            .tv_sec  = timeout_secs,
            .tv_nsec = 0,
        }};

    if (timerfd_settime(timerfd, 0, &spec, nullptr) < 0)
        throw std::runtime_error("failed to set timerfd: " + std::to_string(errno));

    std::optional<TestResult> client_result = std::nullopt;
    std::optional<TestResult> server_result = std::nullopt;

    std::vector<pollfd> pfds = {
        {.fd = timerfd, .events = POLL_IN, .revents = 0},
        {
            .fd      = client_rd,
            .events  = POLL_IN,
            .revents = 0,
        },
        {
            .fd      = server_rd,
            .events  = POLL_IN,
            .revents = 0,
        },
    };

    for (;;) {
        auto n = poll(pfds.data(), pfds.size(), -1);
        if (n < 0)
            throw std::runtime_error("failed to poll: " + std::to_string(errno));

        for (pollfd& pfd: pfds) {
            if (pfd.revents == 0)
                continue;

            if (pfd.fd == timerfd) {
                // the client and server ran out of time, kill them
                kill(client_pid, SIGKILL);
                kill(server_pid, SIGKILL);
                return TestResult::Failure;
            } else if (pfd.fd == client_rd) {
                // we're done with this fd, remove it from the poll list
                pfds.erase(
                    std::remove_if(pfds.begin(), pfds.end(), [&](auto pfd) { return pfd.fd == client_rd; }),
                    pfds.end());

                char buffer = 0;
                if (client_rd.read(&buffer, sizeof(TestResult)) <= 0)
                    client_result = TestResult::Failure;
                else
                    client_result = (TestResult)buffer;

                // this goto is used to break out of two levels of looping and avoids a boolean flag
                if (server_result)
                    goto done;
            } else if (pfd.fd == server_rd) {
                pfds.erase(
                    std::remove_if(pfds.begin(), pfds.end(), [&](auto pfd) { return pfd.fd == server_rd; }),
                    pfds.end());

                char buffer = 0;
                if (server_rd.read(&buffer, sizeof(TestResult)) <= 0)
                    server_result = TestResult::Failure;
                else
                    server_result = (TestResult)buffer;

                if (client_result)
                    goto done;
            }
        }
    }

done:
    int status = 0;
    if (waitpid(client_pid, &status, 0) < 0)
        throw std::runtime_error("failed to wait on client process: " + std::to_string(errno));

    if (waitpid(server_pid, &status, 0) < 0)
        throw std::runtime_error("failed to wait on client process: " + std::to_string(errno));

    if (client_result == TestResult::Success && server_result == TestResult::Success)
        return TestResult::Success;

    return TestResult::Failure;
}

static void handler(int signo)
{
    std::println(std::cerr, "Received signal: {}", signo);
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

#pragma once

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <ifaddrs.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <signal.h>
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
#include <ctime>
#include <exception>
#include <functional>
#include <future>
#include <initializer_list>
#include <iostream>
#include <map>
#include <memory>
#include <numeric>
#include <optional>
#include <print>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#define RETURN_FAILURE_IF_TRUE(condition) \
    if (!(condition)) {                   \
        return TestResult::Failure;       \
    }

using namespace std::chrono_literals;

enum class TestResult : char { Success = 1, Failure = 2 };

inline const char* check_localhost(const char* sAddr)
{
    return std::strcmp(sAddr, "localhost") == 0 ? "127.0.0.1" : sAddr;
}

class OwnedFd {
public:
    int fd;

    OwnedFd(int fd): fd(fd) {}

    // move-only
    OwnedFd(const OwnedFd&)            = delete;
    OwnedFd& operator=(const OwnedFd&) = delete;
    OwnedFd(OwnedFd&& other) noexcept: fd(other.fd) { other.fd = 0; }
    OwnedFd& operator=(OwnedFd&& other) noexcept
    {
        if (this != &other) {
            this->fd = other.fd;
            other.fd = 0;
        }
        return *this;
    }

    ~OwnedFd()
    {
        if (close(fd) < 0)
            std::println(std::cerr, "failed to close fd!");
    }

    size_t write(const void* data, size_t len)
    {
        auto n = ::write(this->fd, data, len);
        if (n < 0)
            throw std::runtime_error("failed to write to socket: " + std::to_string(errno));

        return n;
    }

    void write_all(const char* data, size_t len)
    {
        for (size_t cursor = 0; cursor < len;)
            cursor += this->write(data + cursor, len - cursor);
    }

    void write_all(std::vector<char> data) { this->write_all(data.data(), data.size()); }

    size_t read(void* buffer, size_t len)
    {
        auto n = ::read(this->fd, buffer, len);
        if (n < 0)
            throw std::runtime_error("failed to read from socket: " + std::to_string(errno));
        return n;
    }

    void read_exact(char* buffer, size_t len)
    {
        for (size_t cursor = 0; cursor < len;)
            cursor += this->read(buffer + cursor, len - cursor);
    }

    operator int() { return fd; }
};

class Socket: public OwnedFd {
public:
    Socket(int fd): OwnedFd(fd) {}

    void connect(const char* sAddr, uint16_t port, bool nowait = false)
    {
        sockaddr_in addr {
            .sin_family = AF_INET,
            .sin_port   = htons(port),
            .sin_addr   = {.s_addr = inet_addr(check_localhost(sAddr))},
            .sin_zero   = {0}};

    connect:
        if (::connect(this->fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
            if (errno == ECONNREFUSED && !nowait) {
                std::this_thread::sleep_for(300ms);
                goto connect;
            }

            throw std::runtime_error("failed to connect: " + std::to_string(errno));
        }
    }

    void bind(const char* sAddr, int port)
    {
        sockaddr_in addr {
            .sin_family = AF_INET,
            .sin_port   = htons(port),
            .sin_addr   = {.s_addr = inet_addr(check_localhost(sAddr))},
            .sin_zero   = {0}};

        auto status = ::bind(this->fd, (sockaddr*)&addr, sizeof(addr));
        if (status < 0)
            throw std::runtime_error("failed to bind: " + std::to_string(errno));
    }

    void listen(int n = 32)
    {
        auto status = ::listen(this->fd, n);
        if (status < 0)
            throw std::runtime_error("failed to listen on socket");
    }

    std::pair<Socket, sockaddr_in> accept(int flags = 0)
    {
        sockaddr_in peer_addr {};
        socklen_t len = sizeof(peer_addr);
        auto fd       = ::accept4(this->fd, (sockaddr*)&peer_addr, &len, flags);
        if (fd < 0)
            throw std::runtime_error("failed to accept socket");

        return std::make_pair(Socket(fd), peer_addr);
    }
};

class TcpSocket: public Socket {
public:
    TcpSocket(): Socket(0)
    {
        this->fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (this->fd < 0)
            throw std::runtime_error("failed to create socket");

        int on = 1;
        if (setsockopt(this->fd, IPPROTO_TCP, TCP_NODELAY, (char*)&on, sizeof(on)) < 0)
            throw std::runtime_error("failed to set TCP_NODELAY");
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
        std::println(stderr, "Exception: {}", e.what());
        result = TestResult::Failure;
    }

    pipe_wr.write_all((char*)&result, sizeof(TestResult));
}

// this function along with `wait_for_python_ready_2`
// work together to wait on a signal from the python process
// indicating that the tuntap interface has been created, and that the mitm is ready
inline void wait_for_python_ready_1()
{
    sigset_t set {};
    int sig = 0;

    if (sigemptyset(&set) < 0)
        throw std::runtime_error("failed to create empty signal set");

    if (sigaddset(&set, SIGUSR1) < 0)
        throw std::runtime_error("failed to add sigusr1 to the signal set");

    if (sigprocmask(SIG_BLOCK, &set, nullptr) < 0)
        throw std::runtime_error("failed to mask sigusr1");

    std::println("blocked signal...");
}

inline void wait_for_python_ready_2(int timeout_secs)
{
    sigset_t set {};
    siginfo_t sig {};

    if (sigemptyset(&set) < 0)
        throw std::runtime_error("failed to create empty signal set");

    if (sigaddset(&set, SIGUSR1) < 0)
        throw std::runtime_error("failed to add sigusr1 to the signal set");

    std::println("waiting for python to be ready...");
    timespec ts {.tv_sec = timeout_secs, .tv_nsec = 0};
    if (sigtimedwait(&set, &sig, &ts) < 0)
        throw std::runtime_error("failed to wait on sigusr1");

    sigprocmask(SIG_UNBLOCK, &set, nullptr);
    std::println("signal received; python is ready");
}

// run a test
// forks and runs each of the provided closures
// if `wait_for_python` is true, wait for SIGUSR1 after forking and executing the first closure
inline TestResult test(
    int timeout_secs, std::vector<std::function<TestResult()>> closures, bool wait_for_python = false)
{
    std::vector<std::pair<int, int>> pipes {};
    std::vector<int> pids {};
    for (size_t i = 0; i < closures.size(); i++) {
        int pipe[2] = {0};
        if (pipe2(pipe, O_NONBLOCK) < 0) {
            // close all pipes
            for (auto pipe: pipes) {
                close(pipe.first);
                close(pipe.second);
            }

            throw std::runtime_error("failed to create pipe: " + std::to_string(errno));
        }
        pipes.push_back(std::make_pair(pipe[0], pipe[1]));
    }

    for (size_t i = 0; i < closures.size(); i++) {
        if (wait_for_python && i == 0)
            wait_for_python_ready_1();

        auto pid = fork();
        if (pid < 0) {
            // close all pipes
            for (auto pipe: pipes) {
                close(pipe.first);
                close(pipe.second);
            }

            for (auto pid: pids)
                kill(pid, SIGKILL);

            throw std::runtime_error("failed to fork: " + std::to_string(errno));
        }

        if (pid == 0) {
            // close all pipes except our write half
            for (size_t j = 0; j < pipes.size(); j++) {
                if (i == j)
                    close(pipes[i].first);
                else {
                    close(pipes[j].first);
                    close(pipes[j].second);
                }
            }

            fork_wrapper(closures[i], timeout_secs, pipes[i].second);
            std::exit(EXIT_SUCCESS);
        }

        pids.push_back(pid);

        if (wait_for_python && i == 0)
            wait_for_python_ready_2(3);
    }

    // close all write halves of the pipes
    for (auto pipe: pipes)
        close(pipe.second);

    std::vector<pollfd> pfds {};

    OwnedFd timerfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
    if (timerfd < 0) {
        // close all pipes
        for (auto pipe: pipes)
            close(pipe.first);

        // kill all procs
        for (auto pid: pids)
            kill(pid, SIGKILL);

        throw std::runtime_error("failed to create timerfd: " + std::to_string(errno));
    }

    pfds.push_back({.fd = timerfd.fd, .events = POLL_IN, .revents = 0});
    for (auto pipe: pipes)
        pfds.push_back({
            .fd      = pipe.first,
            .events  = POLL_IN,
            .revents = 0,
        });

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

    if (timerfd_settime(timerfd, 0, &spec, nullptr) < 0) {
        // close all pipes
        for (auto pipe: pipes)
            close(pipe.first);

        // kill all procs
        for (auto pid: pids)
            kill(pid, SIGKILL);

        throw std::runtime_error("failed to set timerfd: " + std::to_string(errno));
    }

    std::vector<std::optional<TestResult>> results(pids.size(), std::nullopt);

    for (;;) {
        auto n = poll(pfds.data(), pfds.size(), -1);
        if (n < 0) {
            // close all pipes
            for (auto pipe: pipes)
                close(pipe.first);

            // kill all procs
            for (auto pid: pids)
                kill(pid, SIGKILL);
            throw std::runtime_error("failed to poll: " + std::to_string(errno));
        }

        for (auto& pfd: std::vector(pfds)) {
            if (pfd.revents == 0)
                continue;

            // timed out
            if (pfd.fd == timerfd) {
                std::println("Timed out!");

                // close all pipes
                for (auto pipe: pipes)
                    close(pipe.first);

                // kill all procs
                for (auto pid: pids)
                    kill(pid, SIGKILL);
                return TestResult::Failure;
            }

            TestResult result = TestResult::Failure;
            char buffer       = 0;
            if (read(pfd.fd, &buffer, sizeof(TestResult)) <= 0)
                result = TestResult::Failure;
            else
                result = (TestResult)buffer;

            auto elem = std::find_if(pipes.begin(), pipes.end(), [fd = pfd.fd](auto pipe) { return pipe.first == fd; });
            auto idx  = elem - pipes.begin();
            results[idx] = result;

            // this subprocess is done, remove its pipe from the poll fds
            pfds.erase(std::remove_if(pfds.begin(), pfds.end(), [&](auto p) { return p.fd == pfd.fd; }), pfds.end());

            auto done = std::all_of(results.begin(), results.end(), [](auto result) { return result.has_value(); });
            if (done)
                goto end;  // justification for goto: breaks out of two levels of loop
        }
    }

end:

    // close all pipes
    for (auto pipe: pipes)
        close(pipe.first);

    int status = 0;
    for (auto pid: pids)
        if (waitpid(pid, &status, 0) < 0)
            std::println(stderr, "failed to wait on a subprocess");

    return std::reduce(results.begin(), results.end(), TestResult::Success, [](auto acc, auto x) {
        if (acc == TestResult::Failure || x == TestResult::Failure)
            return TestResult::Failure;

        return TestResult::Success;
    });
}

inline TestResult run_python(const char* path, std::vector<const wchar_t*> argv = {})
{
    // insert the pid at the start of the argv, this is important for signalling readiness
    pid_t pid   = getppid();
    auto pid_ws = std::to_wstring(pid);
    argv.insert(argv.begin(), pid_ws.c_str());

    PyStatus status;
    PyConfig config;
    PyConfig_InitPythonConfig(&config);

    status = PyConfig_SetBytesString(&config, &config.program_name, "mitm");
    if (PyStatus_Exception(status))
        goto exception;

    status = Py_InitializeFromConfig(&config);
    if (PyStatus_Exception(status))
        goto exception;
    PyConfig_Clear(&config);

    argv.insert(argv.begin(), L"mitm");
    PySys_SetArgv(argv.size(), (wchar_t**)argv.data());

    {
        auto file = fopen(path, "r");
        if (!file) {
            std::println("failed to open file: {}; {}", path, errno);
            return TestResult::Failure;
        }

        PyRun_SimpleFile(file, path);
        fclose(file);
    }

    if (Py_FinalizeEx() < 0) {
        std::println("finalization failure");
        return TestResult::Failure;
    }

    return TestResult::Success;

exception:
    PyConfig_Clear(&config);
    Py_ExitStatusException(status);

    return TestResult::Failure;
}

inline TestResult run_mitm(const wchar_t* testcase, std::vector<const wchar_t*> argv = {})
{
    argv.insert(argv.begin(), testcase);
    return run_python("tests/cc_ymq/py_mitm/runner.py", argv);
}

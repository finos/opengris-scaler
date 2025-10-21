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
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <exception>
#include <filesystem>
#include <format>
#include <functional>
#include <iostream>
#include <optional>
#include <string>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

using namespace std::chrono_literals;

enum class TestResult : char { Success = 1, Failure = 2 };

inline TestResult return_failure_if_false(
    bool cond, const char* msg = nullptr, const char* cond_str = nullptr, const char* file = nullptr, int line = 0)
{
    // Failure: ... (assertion failed) at file:line
    if (!cond) {
        std::cerr << "Failure";
        if (cond_str)
            std::cerr << ": " << cond_str;
        if (msg)
            std::cerr << " (" << msg << ")";
        else
            std::cerr << " (assertion failed)";
        if (file)
            std::cerr << " at " << file << ":" << line;
        std::cerr << '\n';
        return TestResult::Failure;
    }
    return TestResult::Success;
}

// in the case that there's no msg, delegate
inline TestResult return_failure_if_false(bool cond, const char* cond_str, const char* file, int line)
{
    return return_failure_if_false(cond, nullptr, cond_str, file, line);
}

#define RETURN_FAILURE_IF_FALSE(cond, ...)                                                                \
    if (return_failure_if_false((cond), ##__VA_ARGS__, #cond, __FILE__, __LINE__) == TestResult::Failure) \
        return TestResult::Failure;

inline const char* check_localhost(const char* host)
{
    return std::strcmp(host, "localhost") == 0 ? "127.0.0.1" : host;
}

inline std::string format_address(std::string host, uint16_t port)
{
    return std::format("tcp://{}:{}", check_localhost(host.c_str()), port);
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
        if (fd > 0 && close(fd) < 0)
            std::cerr << "failed to close fd!" << std::endl;
    }

    size_t write(const void* data, size_t len)
    {
        auto n = ::write(this->fd, data, len);
        if (n < 0)
            throw std::system_error(errno, std::generic_category(), "failed to write to socket");

        return n;
    }

    void write_all(const char* data, size_t len)
    {
        for (size_t cursor = 0; cursor < len;)
            cursor += this->write(data + cursor, len - cursor);
    }

    void write_all(std::string data) { this->write_all(data.data(), data.length()); }

    void write_all(std::vector<char> data) { this->write_all(data.data(), data.size()); }

    size_t read(void* buffer, size_t len)
    {
        auto n = ::read(this->fd, buffer, len);
        if (n < 0)
            throw std::system_error(errno, std::generic_category(), "failed to read from socket");
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

    void connect(const char* host, uint16_t port, bool nowait = false)
    {
        sockaddr_in addr {
            .sin_family = AF_INET,
            .sin_port   = htons(port),
            .sin_addr   = {.s_addr = inet_addr(check_localhost(host))},
            .sin_zero   = {0}};

    connect:
        if (::connect(this->fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
            if (errno == ECONNREFUSED && !nowait) {
                std::this_thread::sleep_for(300ms);
                goto connect;
            }

            throw std::system_error(errno, std::generic_category(), "failed to connect");
        }
    }

    void bind(const char* host, int port)
    {
        sockaddr_in addr {
            .sin_family = AF_INET,
            .sin_port   = htons(port),
            .sin_addr   = {.s_addr = inet_addr(check_localhost(host))},
            .sin_zero   = {0}};

        auto status = ::bind(this->fd, (sockaddr*)&addr, sizeof(addr));
        if (status < 0)
            throw std::system_error(errno, std::generic_category(), "failed to bind");
    }

    void listen(int n = 32)
    {
        auto status = ::listen(this->fd, n);
        if (status < 0)
            throw std::system_error(errno, std::generic_category(), "failed to listen on socket");
    }

    std::pair<Socket, sockaddr_in> accept(int flags = 0)
    {
        sockaddr_in peer_addr {};
        socklen_t len = sizeof(peer_addr);
        auto fd       = ::accept4(this->fd, (sockaddr*)&peer_addr, &len, flags);
        if (fd < 0)
            throw std::system_error(errno, std::generic_category(), "failed to accept socket");

        return std::make_pair(Socket(fd), peer_addr);
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

class TcpSocket: public Socket {
public:
    TcpSocket(bool nodelay = true): Socket(0)
    {
        this->fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (this->fd < 0)
            throw std::system_error(errno, std::generic_category(), "failed to create socket");

        int on = 1;
        if (nodelay && setsockopt(this->fd, IPPROTO_TCP, TCP_NODELAY, (char*)&on, sizeof(on)) < 0)
            throw std::system_error(errno, std::generic_category(), "failed to set nodelay");

        if (setsockopt(this->fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) < 0)
            throw std::system_error(errno, std::generic_category(), "failed to set reuseaddr");
    }

    void flush()
    {
        int on  = 1;
        int off = 0;

        if (setsockopt(this->fd, IPPROTO_TCP, TCP_NODELAY, (char*)&off, sizeof(off)) < 0)
            throw std::system_error(errno, std::generic_category(), "failed to set nodelay");

        if (setsockopt(this->fd, IPPROTO_TCP, TCP_NODELAY, (char*)&on, sizeof(on)) < 0)
            throw std::system_error(errno, std::generic_category(), "failed to set nodelay");

        if (setsockopt(this->fd, IPPROTO_TCP, TCP_NODELAY, (char*)&off, sizeof(off)) < 0)
            throw std::system_error(errno, std::generic_category(), "failed to set nodelay");

        if (setsockopt(this->fd, IPPROTO_TCP, TCP_NODELAY, (char*)&on, sizeof(on)) < 0)
            throw std::system_error(errno, std::generic_category(), "failed to set nodelay");
    }
};

inline void fork_wrapper(std::function<TestResult()> fn, int timeout_secs, OwnedFd pipe_wr)
{
    TestResult result = TestResult::Failure;
    try {
        result = fn();
    } catch (const std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
        result = TestResult::Failure;
    } catch (...) {
        std::cerr << "Unknown exception" << std::endl;
        result = TestResult::Failure;
    }

    pipe_wr.write_all((char*)&result, sizeof(TestResult));
}

// this function along with `wait_for_python_ready_sigwait()`
// work together to wait on a signal from the python process
// indicating that the tuntap interface has been created, and that the mitm is ready
inline void wait_for_python_ready_sigblock()
{
    sigset_t set {};

    if (sigemptyset(&set) < 0)
        throw std::system_error(errno, std::generic_category(), "failed to create empty signal set");

    if (sigaddset(&set, SIGUSR1) < 0)
        throw std::system_error(errno, std::generic_category(), "failed to add sigusr1 to the signal set");

    if (sigprocmask(SIG_BLOCK, &set, nullptr) < 0)
        throw std::system_error(errno, std::generic_category(), "failed to mask sigusr1");

    std::cout << "blocked signal..." << std::endl;
}

inline void wait_for_python_ready_sigwait(int timeout_secs)
{
    sigset_t set {};
    siginfo_t sig {};

    if (sigemptyset(&set) < 0)
        throw std::system_error(errno, std::generic_category(), "failed to create empty signal set");

    if (sigaddset(&set, SIGUSR1) < 0)
        throw std::system_error(errno, std::generic_category(), "failed to add sigusr1 to the signal set");

    std::cout << "waiting for python to be ready..." << std::endl;
    timespec ts {.tv_sec = timeout_secs, .tv_nsec = 0};
    if (sigtimedwait(&set, &sig, &ts) < 0)
        throw std::system_error(errno, std::generic_category(), "failed to wait on sigusr1");

    sigprocmask(SIG_UNBLOCK, &set, nullptr);
    std::cout << "signal received; python is ready" << std::endl;
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
            std::for_each(pipes.begin(), pipes.end(), [](const auto& pipe) {
                close(pipe.first);
                close(pipe.second);
            });

            throw std::system_error(errno, std::generic_category(), "failed to create pipe: ");
        }
        pipes.push_back(std::make_pair(pipe[0], pipe[1]));
    }

    for (size_t i = 0; i < closures.size(); i++) {
        if (wait_for_python && i == 0)
            wait_for_python_ready_sigblock();

        auto pid = fork();
        if (pid < 0) {
            std::for_each(pipes.begin(), pipes.end(), [](const auto& pipe) {
                close(pipe.first);
                close(pipe.second);
            });

            std::for_each(pids.begin(), pids.end(), [](const auto& pid) { kill(pid, SIGKILL); });

            throw std::system_error(errno, std::generic_category(), "failed to fork");
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
            wait_for_python_ready_sigwait(3);
    }

    // close all write halves of the pipes
    for (auto pipe: pipes)
        close(pipe.second);

    std::vector<pollfd> pfds {};

    OwnedFd timerfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
    if (timerfd < 0) {
        std::for_each(pipes.begin(), pipes.end(), [](const auto& pipe) { close(pipe.first); });
        std::for_each(pids.begin(), pids.end(), [](const auto& pid) { kill(pid, SIGKILL); });

        throw std::system_error(errno, std::generic_category(), "failed to create timerfd");
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
        std::for_each(pipes.begin(), pipes.end(), [](const auto& pipe) { close(pipe.first); });
        std::for_each(pids.begin(), pids.end(), [](const auto& pid) { kill(pid, SIGKILL); });

        throw std::system_error(errno, std::generic_category(), "failed to set timerfd");
    }

    std::vector<std::optional<TestResult>> results(pids.size(), std::nullopt);

    for (;;) {
        auto n = poll(pfds.data(), pfds.size(), -1);
        if (n < 0) {
            std::for_each(pipes.begin(), pipes.end(), [](const auto& pipe) { close(pipe.first); });
            std::for_each(pids.begin(), pids.end(), [](const auto& pid) { kill(pid, SIGKILL); });

            throw std::system_error(errno, std::generic_category(), "failed to poll: ");
        }

        for (auto& pfd: std::vector(pfds)) {
            if (pfd.revents == 0)
                continue;

            // timed out
            if (pfd.fd == timerfd) {
                std::cout << "Timed out!\n";

                std::for_each(pipes.begin(), pipes.end(), [](const auto& pipe) { close(pipe.first); });
                std::for_each(pids.begin(), pids.end(), [](const auto& pid) { kill(pid, SIGKILL); });

                return TestResult::Failure;
            }

            auto elem = std::find_if(pipes.begin(), pipes.end(), [fd = pfd.fd](auto pipe) { return pipe.first == fd; });
            auto idx  = elem - pipes.begin();

            TestResult result = TestResult::Failure;
            char buffer       = 0;
            auto n            = read(pfd.fd, &buffer, sizeof(TestResult));
            if (n == 0) {
                std::cout << "failed to read from pipe: pipe closed unexpectedly\n";
                result = TestResult::Failure;
            } else if (n < 0) {
                std::cout << "failed to read from pipe: " << std::strerror(errno) << std::endl;
                result = TestResult::Failure;
            } else
                result = (TestResult)buffer;

            // the subprocess should have exited
            // check its exit status
            int status;
            if (waitpid(pids[idx], &status, 0) < 0)
                std::cout << "failed to wait on subprocess[" << idx << "]: " << std::strerror(errno) << std::endl;

            auto exit_status = WEXITSTATUS(status);
            if (WIFEXITED(status) && exit_status != EXIT_SUCCESS) {
                std::cout << "subprocess[" << idx << "] exited with status " << exit_status << std::endl;
            } else if (WIFSIGNALED(status)) {
                std::cout << "subprocess[" << idx << "] killed by signal " << WTERMSIG(status) << std::endl;
            } else {
                std::cout << "subprocess[" << idx << "] completed with "
                          << (result == TestResult::Success ? "Success" : "Failure") << std::endl;
            }

            // store the result
            results[idx] = result;

            // this subprocess is done, remove its pipe from the poll fds
            pfds.erase(std::remove_if(pfds.begin(), pfds.end(), [&](auto p) { return p.fd == pfd.fd; }), pfds.end());

            auto done = std::all_of(results.begin(), results.end(), [](auto result) { return result.has_value(); });
            if (done)
                goto end;  // justification for goto: breaks out of two levels of loop
        }
    }

end:

    std::for_each(pipes.begin(), pipes.end(), [](const auto& pipe) { close(pipe.first); });

    if (std::ranges::any_of(results, [](auto x) { return x == TestResult::Failure; }))
        return TestResult::Failure;

    return TestResult::Success;
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

    argv.insert(argv.begin(), L"mitm");
    status = PyConfig_SetArgv(&config, argv.size(), (wchar_t**)argv.data());
    if (PyStatus_Exception(status))
        goto exception;

    // pass argv to the script as-is
    config.parse_argv = 0;

    status = Py_InitializeFromConfig(&config);
    if (PyStatus_Exception(status))
        goto exception;
    PyConfig_Clear(&config);

    // add the cwd to the path
    {
        PyObject* sysPath = PySys_GetObject("path");
        PyObject* newPath = PyUnicode_FromString(".");
        PyList_Append(sysPath, newPath);
        Py_DECREF(newPath);
    }

    {
        auto file = fopen(path, "r");
        if (!file)
            throw std::system_error(errno, std::generic_category(), "failed to open python file");

        PyRun_SimpleFile(file, path);
        fclose(file);
    }

    if (Py_FinalizeEx() < 0) {
        std::cerr << "finalization failure" << std::endl;
        return TestResult::Failure;
    }

    return TestResult::Success;

exception:
    PyConfig_Clear(&config);
    Py_ExitStatusException(status);

    return TestResult::Failure;
}

// change the current working directory to the project root
// this is important for finding the python mitm script
inline void chdir_to_project_root()
{
    auto cwd = std::filesystem::current_path();

    // if pyproject.toml is in `path`, it's the project root
    for (auto path = cwd; !path.empty(); path = path.parent_path()) {
        if (std::filesystem::exists(path / "pyproject.toml")) {
            // change to the project root
            std::filesystem::current_path(path);
            return;
        }
    }
}

inline TestResult run_mitm(
    std::string testcase,
    std::string mitm_ip,
    uint16_t mitm_port,
    std::string remote_ip,
    uint16_t remote_port,
    std::vector<std::string> extra_args = {})
{
    auto cwd = std::filesystem::current_path();
    chdir_to_project_root();

    // we build the args for the user to make calling the function more convenient
    std::vector<std::string> args {
        testcase, mitm_ip, std::to_string(mitm_port), remote_ip, std::to_string(remote_port)};

    for (auto arg: extra_args)
        args.push_back(arg);

    // we need to convert to wide strings to pass to Python
    std::vector<std::wstring> wide_args_owned {};

    // the strings are ascii so we can just make them into wstrings
    for (const auto& str: args)
        wide_args_owned.emplace_back(str.begin(), str.end());

    std::vector<const wchar_t*> wide_args {};
    for (const auto& wstr: wide_args_owned)
        wide_args.push_back(wstr.c_str());

    auto result = run_python("tests/cpp/ymq/py_mitm/main.py", wide_args);

    // change back to the original working directory
    std::filesystem::current_path(cwd);
    return result;
}

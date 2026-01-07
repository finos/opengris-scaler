#pragma once

#define PY_SSIZE_T_CLEAN

// if on Windows and in debug mode, undefine _DEBUG before including Python.h
// this prevents issues including the debug version of the Python library
#if defined(_WIN32) && defined(_DEBUG)
#undef _DEBUG
#include <Python.h>
#define _DEBUG
#else
#include <Python.h>
#endif

#include <fcntl.h>
#include <signal.h>
#include <sys/types.h>

#ifdef __linux__
#include <arpa/inet.h>
#include <ifaddrs.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <sys/poll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/timerfd.h>
#include <sys/wait.h>
#include <unistd.h>
#endif  // __linux__
#ifdef _WIN32
#include <io.h>
#include <windows.h>
#include <winsock2.h>
#include <ws2tcpip.h>

// the windows timer apis work in 100-nanosecond units
const LONGLONG ns_per_second = 1'000'000'000LL;
const LONGLONG ns_per_unit   = 100LL;  // 1 unit = 100 nanoseconds

#define popen  _popen
#define pclose _pclose
#endif  // _WIN32

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
#include <fstream>
#include <functional>
#include <iostream>
#include <optional>
#include <string>
#include <utility>

#include "tests/cpp/ymq/common/utils.h"
#include "tests/cpp/ymq/pipe/pipe.h"
#include "tests/cpp/ymq/pipe/pipe_writer.h"

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

// hEvent: unused on linux, event handle on windows
inline void fork_wrapper(std::function<TestResult()> fn, int timeout_secs, PipeWriter pipe_wr, void* hEvent)
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

    pipe_wr.writeAll((char*)&result, sizeof(TestResult));

#ifdef _WIN32
    SetEvent((HANDLE)hEvent);
#endif  // _WIN32
}

// this function along with `wait_for_python_ready_sigwait()`
// work together to wait on a signal from the python process
// indicating that the tuntap interface has been created, and that the mitm is ready
//
// hEvent is an output parameter for windows but unused on linux
inline void wait_for_python_ready_sigblock(void** hEvent)
{
#ifdef __linux__
    sigset_t set {};

    if (sigemptyset(&set) < 0)
        raise_system_error("failed to create empty signal set");

    if (sigaddset(&set, SIGUSR1) < 0)
        raise_system_error("failed to add sigusr1 to the signal set");

    if (sigprocmask(SIG_BLOCK, &set, nullptr) < 0)
        raise_system_error("failed to mask sigusr1");

#endif  // __linux__
#ifdef _WIN32
    // TODO: implement signaling of this event in the python mitm
    *hEvent = CreateEvent(
        NULL,                     // default security attributes
        FALSE,                    // auto-reset event
        FALSE,                    // initial state is nonsignaled
        "Global\\PythonSignal");  // name of the event
    if (*hEvent == NULL)
        raise_system_error("failed to create event");
#endif  // _WIN32

    std::cout << "blocked signal..." << std::endl;
}

// as in the above function, hEvent is unused on linux
inline void wait_for_python_ready_sigwait(void* hEvent, int timeout_secs)
{
    std::cout << "waiting for python to be ready..." << std::endl;

#ifdef __linux__
    timespec ts {.tv_sec = timeout_secs, .tv_nsec = 0};
    sigset_t set {};
    siginfo_t sig {};

    if (sigemptyset(&set) < 0)
        raise_system_error("failed to create empty signal set");

    if (sigaddset(&set, SIGUSR1) < 0)
        raise_system_error("failed to add sigusr1 to the signal set");

    if (sigtimedwait(&set, &sig, &ts) < 0)
        raise_system_error("failed to wait on sigusr1");

    sigprocmask(SIG_UNBLOCK, &set, nullptr);

#endif  // __linux__
#ifdef _WIN32
    DWORD waitResult = WaitForSingleObject(hEvent, timeout_secs * 1000);
    if (waitResult != WAIT_OBJECT_0) {
        raise_system_error("failed to wait on event");
    }
    CloseHandle(hEvent);
#endif  // _WIN32

    std::cout << "signal received; python is ready" << std::endl;
}

// run a test
// forks and runs each of the provided closures
// if `wait_for_python` is true, wait for SIGUSR1 after forking and executing the first closure
inline TestResult test(
    int timeout_secs, std::vector<std::function<TestResult()>> closures, bool wait_for_python = false)
{
    std::vector<Pipe> pipes {};

    for (size_t i = 0; i < closures.size(); i++)
        pipes.emplace_back();

#ifdef __linux__
    std::vector<int> pids {};
    void* hEvent = nullptr;
    for (size_t i = 0; i < closures.size(); i++) {
        if (wait_for_python && i == 0)
            wait_for_python_ready_sigblock(&hEvent);

        auto pid = fork();
        if (pid < 0) {
            std::for_each(pids.begin(), pids.end(), [](const auto& pid) { kill(pid, SIGKILL); });

            raise_system_error("failed to fork");
        }

        if (pid == 0) {
            fork_wrapper(closures[i], timeout_secs, std::move(pipes[i].writer), nullptr);
            std::exit(EXIT_SUCCESS);
        }

        pids.push_back(pid);

        if (wait_for_python && i == 0)
            wait_for_python_ready_sigwait(&hEvent, 3);
    }

    std::vector<pollfd> pfds {};

    int timerfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
    if (timerfd < 0) {
        std::for_each(pids.begin(), pids.end(), [](const auto& pid) { kill(pid, SIGKILL); });

        raise_system_error("failed to create timerfd");
    }

    pfds.push_back({.fd = timerfd, .events = POLL_IN, .revents = 0});
    for (const auto& pipe: pipes)
        pfds.push_back({
            .fd      = (int)pipe.reader.fd(),
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
        std::for_each(pids.begin(), pids.end(), [](const auto& pid) { kill(pid, SIGKILL); });
        close(timerfd);

        raise_system_error("failed to set timerfd");
    }

    std::vector<std::optional<TestResult>> results(pids.size(), std::nullopt);

    for (;;) {
        auto n = poll(pfds.data(), pfds.size(), -1);
        if (n < 0) {
            std::for_each(pids.begin(), pids.end(), [](const auto& pid) { kill(pid, SIGKILL); });
            close(timerfd);

            raise_system_error("failed to poll");
        }

        for (auto& pfd: std::vector(pfds)) {
            if (pfd.revents == 0)
                continue;

            // timed out
            if (pfd.fd == timerfd) {
                std::cout << "Timed out!\n";

                std::for_each(pids.begin(), pids.end(), [](const auto& pid) { kill(pid, SIGKILL); });
                close(timerfd);

                return TestResult::Failure;
            }

            auto elem = std::find_if(
                pipes.begin(), pipes.end(), [fd = pfd.fd](const auto& pipe) { return pipe.reader.fd() == fd; });
            auto idx = elem - pipes.begin();

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
            pfds.erase(
                std::remove_if(pfds.begin(), pfds.end(), [&](const auto& p) { return p.fd == pfd.fd; }), pfds.end());

            auto done =
                std::all_of(results.begin(), results.end(), [](const auto& result) { return result.has_value(); });
            if (done)
                goto end;  // justification for goto: breaks out of two levels of loop
        }
    }

end:
    close(timerfd);

    if (std::ranges::any_of(results, [](const auto& x) { return x == TestResult::Failure; }))
        return TestResult::Failure;

    return TestResult::Success;
#endif  // __linux__
#ifdef _WIN32
    std::vector<HANDLE> events {};
    std::vector<std::jthread> threads {};

    for (size_t i = 0; i < closures.size(); i++) {
        HANDLE hEvent = CreateEvent(
            nullptr,   // default security attributes
            true,      // auto-reset event
            false,     // initial state is nonsignaled
            nullptr);  // unnamed event
        if (!hEvent)
            raise_system_error("failed to create event");
        events.push_back(hEvent);
    }

    for (size_t i = 0; i < closures.size(); i++) {
        HANDLE hEvent = nullptr;
        if (wait_for_python && i == 0)
            wait_for_python_ready_sigblock(&hEvent);

        threads.emplace_back(fork_wrapper, closures[i], timeout_secs, std::move(pipes[i].writer), events[i]);

        if (wait_for_python && i == 0)
            wait_for_python_ready_sigwait(hEvent, 3);
    }

    HANDLE timer = CreateWaitableTimer(nullptr, true, nullptr);
    if (!timer) {
        std::for_each(events.begin(), events.end(), [](const auto& ev) { CloseHandle(ev); });
        raise_system_error("failed to create waitable timer");
    }

    LARGE_INTEGER expires_in = {0};

    // negative value indicates relative time
    expires_in.QuadPart = -static_cast<LONGLONG>(timeout_secs) * ns_per_second / ns_per_unit;
    if (!SetWaitableTimer(timer, &expires_in, 0, nullptr, nullptr, false)) {
        std::for_each(events.begin(), events.end(), [](const auto& ev) { CloseHandle(ev); });
        CloseHandle(timer);
        raise_system_error("failed to set waitable timer");
    }

    // these are the handles we're going to poll
    std::vector<HANDLE> wait_handles {timer};

    // poll all read halves of the pipes
    for (const auto& ev: events)
        wait_handles.push_back(ev);

    std::vector<std::optional<TestResult>> results(threads.size(), std::nullopt);

    for (;;) {
        DWORD waitResult = WaitForMultipleObjects(wait_handles.size(), wait_handles.data(), false, INFINITE);
        if (waitResult == WAIT_FAILED) {
            std::for_each(events.begin(), events.end(), [](const auto& ev) { CloseHandle(ev); });
            CloseHandle(timer);
            raise_system_error("failed to wait on handles");
        }

        // the idx of the handle in the handles array
        // note that index 0 is the timer
        // and we adjust the handles array as tasks complete
        // so we need an extra step to calculate the index in `closure`-space
        size_t wait_idx = (size_t)waitResult - WAIT_OBJECT_0;

        // timed out
        if (wait_idx == 0) {
            std::cout << "Timed out!\n";
            std::for_each(threads.begin(), threads.end(), [](auto& t) {
                t.request_stop();
                t.detach();
            });
            std::for_each(events.begin(), events.end(), [](const auto& ev) { CloseHandle(ev); });
            CloseHandle(timer);
            return TestResult::Failure;
        }

        // find the idx
        const auto& hEvent = wait_handles[wait_idx];
        auto event_it  = std::find_if(events.begin(), events.end(), [hEvent](const auto& ev) { return ev == hEvent; });
        const auto idx = event_it - events.begin();
        auto& pipe     = pipes[idx];
        TestResult result = TestResult::Failure;
        char buffer       = 0;
        try {
            pipe.reader.readExact(&buffer, sizeof(TestResult));
            result = (TestResult)buffer;
        } catch (const std::system_error& e) {
            std::cout << "failed to read from pipe: " << e.what() << std::endl;
            result = TestResult::Failure;
        }

        std::cout << "subprocess[" << idx << "] completed with "
                  << (result == TestResult::Success ? "Success" : "Failure") << std::endl;

        // store the result
        results[idx] = result;

        // this subprocess is done, remove its pipe from the handles
        wait_handles.erase(
            std::remove_if(wait_handles.begin(), wait_handles.end(), [&](const auto& h) { return h == hEvent; }),
            wait_handles.end());
        auto done = std::all_of(results.begin(), results.end(), [](const auto& result) { return result.has_value(); });
        if (done)
            goto end;  // justification for goto: breaks out of two levels of loop
    }

end:
    std::for_each(events.begin(), events.end(), [](const auto& ev) { CloseHandle(ev); });
    CloseHandle(timer);

    if (std::ranges::any_of(results, [](auto x) { return x == TestResult::Failure; }))
        return TestResult::Failure;

    return TestResult::Success;
#endif  // _WIN32
}

inline std::wstring discover_python_home(std::string command)
{
    // leverage the system's command line to get the current python prefix
    FILE* pipe = popen(std::format("{} -c \"import sys; print(sys.prefix)\"", command).c_str(), "r");
    if (!pipe)
        throw std::runtime_error("failed to start python process to discover prefix");

    std::array<char, 128> buffer {};
    std::string output {};

    size_t n;
    while ((n = fread(buffer.data(), 1, buffer.size(), pipe)) > 0)
        output.append(buffer.data(), n);

    // remove trailing whitespace
    output.erase(output.find_last_not_of("\r\n") + 1);

    auto status = pclose(pipe);
    if (status < 0)
        throw std::runtime_error("failed to close close process");
    else if (status > 0)
        throw std::runtime_error("process returned non-zero exit code: " + std::to_string(status));

    // assume it's ascii, so we can just cast it as a wstring
    return std::wstring(output.begin(), output.end());
}

inline void ensure_python_initialized()
{
    if (Py_IsInitialized())
        return;

#ifdef _WIN32
    auto python_home = discover_python_home("python");
    Py_SetPythonHome(python_home.c_str());
#endif  // _WIN32

    Py_Initialize();

    // add the cwd to the path
    {
        PyObject* sysPath = PySys_GetObject("path");
        if (!sysPath)
            throw std::runtime_error("failed to get sys.path");

        PyObject* newPath = PyUnicode_FromString(".");
        if (!newPath)
            throw std::runtime_error("failed to create Python string");

        if (PyList_Append(sysPath, newPath) < 0) {
            Py_DECREF(newPath);
            throw std::runtime_error("failed to append to sys.path");
        }

        Py_DECREF(newPath);
    }

    // release the GIL, the caller will have to acquire it again
    PyEval_SaveThread();
}

inline void maybe_finalize_python()
{
    PyGILState_STATE gstate = PyGILState_Ensure();
    if (!Py_IsInitialized())
        return;

    Py_Finalize();

    // stop compiler from complaining that it's unused
    (void)gstate;
}

inline TestResult run_python(const char* path, std::vector<std::optional<std::string>> argv = {})
{
    PyGILState_STATE gstate = PyGILState_Ensure();

// insert the pid at the start of the argv, this is important for signalling readiness
#ifdef __linux__
    pid_t pid = getppid();
#endif  // __linux__
#ifdef _WIN32
    DWORD pid = GetCurrentProcessId();
#endif  // _WIN32

    auto pid_s = std::to_string(pid);
    argv.insert(argv.begin(), pid_s.c_str());
    argv.insert(argv.begin(), "mitm");

    // set argv
    {
        PyObject* py_argv = PyList_New(argv.size());
        if (!py_argv)
            goto exception;

        for (size_t i = 0; i < argv.size(); i++)
            if (argv[i])
                PyList_SET_ITEM(py_argv, i, PyUnicode_FromString(argv[i].value().c_str()));
            else
                PyList_SET_ITEM(py_argv, i, Py_None);

        if (PySys_SetObject("argv", py_argv) < 0)
            goto exception;

        Py_DECREF(py_argv);
    }

    {
        std::ifstream file(path);
        std::stringstream buffer;
        buffer << file.rdbuf();

        int rc = PyRun_SimpleString(buffer.str().c_str());
        file.close();

        if (rc < 0)
            throw std::runtime_error("failed to run python script");
    }

    PyGILState_Release(gstate);
    return TestResult::Success;

exception:
    PyGILState_Release(gstate);
    return TestResult::Failure;
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
    std::vector<std::optional<std::string>> args {
        testcase, mitm_ip, std::to_string(mitm_port), remote_ip, std::to_string(remote_port)};

    for (auto arg: extra_args)
        args.push_back(arg);

    auto result = run_python("tests/cpp/ymq/py_mitm/main.py", args);

    // change back to the original working directory
    std::filesystem::current_path(cwd);
    return result;
}

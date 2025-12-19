#pragma once

#include <cstdint>
#include <functional>
#include <optional>
#include <string>

#include "tests/cpp/ymq/pipe/pipe_writer.h"

enum class TestResult : char { Success = 1, Failure = 2 };

TestResult return_failure_if_false(
    bool cond, const char* msg = nullptr, const char* cond_str = nullptr, const char* file = nullptr, int line = 0);

// in the case that there's no msg, delegate
TestResult return_failure_if_false(bool cond, const char* cond_str, const char* file, int line);

#define RETURN_FAILURE_IF_FALSE(cond, ...)                                                                \
    if (return_failure_if_false((cond), ##__VA_ARGS__, #cond, __FILE__, __LINE__) == TestResult::Failure) \
        return TestResult::Failure;

void signal_event(void* hEvent);

// hEvent: unused on linux, event handle on windows
void test_wrapper(std::function<TestResult()> fn, int timeout_secs, PipeWriter pipe_wr, void* hEvent);

// this function along with `wait_for_python_ready_sigwait()`
// work together to wait on a signal from the python process
// indicating that the tuntap interface has been created, and that the mitm is ready
//
// hEvent is an output parameter for windows but unused on linux
void wait_for_python_ready_sigblock(void** hEvent);

// as in the above function, hEvent is unused on linux
void wait_for_python_ready_sigwait(void* hEvent, int timeout_secs);

// run a test
// forks and runs each of the provided closures
// if `wait_for_python` is true, wait for SIGUSR1 after forking and executing the first closure
TestResult test(int timeout_secs, std::vector<std::function<TestResult()>> closures, bool wait_for_python = false);

std::wstring discover_python_home(std::string command);

void ensure_python_home();
void ensure_python_initialized();
void maybe_finalize_python();

// get the pid of the process waiting to be signaled by Python
int get_listener_pid();

TestResult run_python(const char* path, std::vector<std::optional<std::string>> argv = {});

TestResult run_mitm(
    std::string testcase,
    std::string mitm_ip,
    uint16_t mitm_port,
    std::string remote_ip,
    uint16_t remote_port,
    std::vector<std::string> extra_args = {});

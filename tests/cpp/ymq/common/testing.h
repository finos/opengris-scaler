#pragma once

#include <cstdint>
#include <functional>
#include <optional>
#include <string>

#include "tests/cpp/ymq/pipe/pipe_writer.h"

enum class TestResult : char { Success = 1, Failure = 2 };

TestResult returnFailureIfFalse(bool cond, const char* condStr, const char* file, int line, const char* msg = nullptr);

#define RETURN_FAILURE_IF_FALSE(cond, ...)                                                             \
    if (returnFailureIfFalse((cond), #cond, __FILE__, __LINE__, ##__VA_ARGS__) == TestResult::Failure) \
        return TestResult::Failure;

void signalEvent(void* hEvent);

// hEvent: unused on linux, event handle on windows
void testWrapper(std::function<TestResult()> fn, int timeoutSecs, PipeWriter pipeWr, void* hEvent);

// this function along with `waitForPythonReadySigwait()`
// work together to wait on a signal from the python process
// indicating that the tuntap interface has been created, and that the mitm is ready
//
// hEvent is an output parameter for windows but unused on linux
void waitForPythonReadySigblock(void** hEvent);

// as in the above function, hEvent is unused on linux
void waitForPythonReadySigwait(void* hEvent, int timeoutSecs);

// run a test
// forks and runs each of the provided closures
// if `wait_for_python` is true, wait for SIGUSR1 after forking and executing the first closure
TestResult test(int timeoutSecs, std::vector<std::function<TestResult()>> closures, bool waitForPython = false);

std::wstring discoverPythonHome(std::string command);

void ensurePythonHome();
void ensurePythonInitialized();
void maybeFinalizePython();

// get the pid of the process waiting to be signaled by Python
int getListenerPid();

TestResult runPython(const char* path, std::vector<std::optional<std::string>> argv = {});

struct MitmIPs {
    const char* mitmIp;
    const char* remoteIp;
};

// returns the platform-appropriate IP addresses for MITM tests
MitmIPs getMitmIPs();

TestResult runMitm(
    std::string testCase,
    std::string mitmIp,
    uint16_t mitmPort,
    std::string remoteIp,
    uint16_t remotePort,
    std::vector<std::string> extraArgs = {});
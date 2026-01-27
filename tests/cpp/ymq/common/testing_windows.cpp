#define PY_SSIZE_T_CLEAN

// on Windows and in debug mode, undefine _DEBUG before including Python.h
// this prevents issues including the debug version of the Python library
#ifdef _DEBUG
#undef _DEBUG
#include <Python.h>
#define _DEBUG
#else
#include <Python.h>
#endif

#include <io.h>
#include <windows.h>
#include <winsock2.h>
#include <ws2tcpip.h>

#include <algorithm>
#include <iostream>
#include <thread>

#include "tests/cpp/ymq/common/testing.h"
#include "tests/cpp/ymq/common/utils.h"
#include "tests/cpp/ymq/pipe/pipe.h"

// the windows timer apis work in 100-nanosecond units
const LONGLONG ns_per_second = 1'000'000'000LL;
const LONGLONG ns_per_unit   = 100LL;  // 1 unit = 100 nanoseconds

void ensure_python_home()
{
    auto python_home = discover_python_home("python");
    Py_SetPythonHome(python_home.c_str());
}

int get_listener_pid()
{
    return GetCurrentProcessId();
}

void signal_event(void* hEvent)
{
    SetEvent((HANDLE)hEvent);
}

void wait_for_python_ready_sigblock(void** hEvent)
{
    *hEvent = CreateEvent(
        NULL,                     // default security attributes
        false,                    // auto-reset event
        false,                    // initial state is nonsignaled
        "Global\\PythonSignal");  // name of the event
    if (*hEvent == NULL)
        raise_system_error("failed to create event");

    std::cout << "blocked signal..." << std::endl;
}

void wait_for_python_ready_sigwait(void* hEvent, int timeout_secs)
{
    std::cout << "waiting for python to be ready..." << std::endl;

    DWORD waitResult = WaitForSingleObject(hEvent, timeout_secs * 1000);

    if (waitResult != WAIT_OBJECT_0) {
        raise_system_error("failed to wait on event");
    }

    CloseHandle(hEvent);

    std::cout << "signal received; python is ready" << std::endl;
}

TestResult test(int timeout_secs, std::vector<std::function<TestResult()>> closures, bool wait_for_python)
{
    std::vector<Pipe> pipes {};

    for (size_t i = 0; i < closures.size(); i++)
        pipes.emplace_back();

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

        threads.emplace_back(test_wrapper, closures[i], timeout_secs, std::move(pipes[i].writer), events[i]);

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
}

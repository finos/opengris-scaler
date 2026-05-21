#define PY_SSIZE_T_CLEAN

#include <Python.h>
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

#include <algorithm>
#include <cstring>
#include <iostream>

#include "tests/cpp/ymq/common/testing.h"
#include "tests/cpp/ymq/common/utils.h"
#include "tests/cpp/ymq/pipe/pipe.h"

void ensurePythonHome()
{
    // no-op
}

void signalEvent(void* hEvent)
{
    // no-op
}

int getListenerPid()
{
    return getppid();
}

void waitForPythonReadySigblock(void** hEvent)
{
    sigset_t set {};

    if (sigemptyset(&set) < 0)
        raiseSystemError("failed to create empty signal set");

    if (sigaddset(&set, SIGUSR1) < 0)
        raiseSystemError("failed to add sigusr1 to the signal set");

    if (sigprocmask(SIG_BLOCK, &set, nullptr) < 0)
        raiseSystemError("failed to mask sigusr1");

    std::cout << "blocked signal..." << std::endl;
}

void waitForPythonReadySigwait(void* hEvent, int timeoutSecs)
{
    std::cout << "waiting for python to be ready..." << std::endl;

    timespec ts {.tv_sec = timeoutSecs, .tv_nsec = 0};
    sigset_t set {};
    siginfo_t sig {};

    if (sigemptyset(&set) < 0)
        raiseSystemError("failed to create empty signal set");

    if (sigaddset(&set, SIGUSR1) < 0)
        raiseSystemError("failed to add sigusr1 to the signal set");

    int result {};
    while ((result = sigtimedwait(&set, &sig, &ts)) < 0) {
        if (errno == EINTR) {
            continue;  // Interrupted, retry
        }
        raiseSystemError("failed to wait on sigusr1");
    }

    sigprocmask(SIG_UNBLOCK, &set, nullptr);

    std::cout << "signal received; python is ready" << std::endl;
}

TestResult test(int timeoutSecs, std::vector<std::function<TestResult()>> closures, bool waitForPython)
{
    std::vector<Pipe> pipes {};

    for (size_t i = 0; i < closures.size(); i++)
        pipes.emplace_back();

    std::vector<int> pids {};
    void* hEvent = nullptr;
    for (size_t i = 0; i < closures.size(); i++) {
        if (waitForPython && i == 0)
            waitForPythonReadySigblock(&hEvent);

        auto pid = fork();
        if (pid < 0) {
            std::for_each(pids.begin(), pids.end(), [](const auto& pid) { kill(pid, SIGKILL); });

            raiseSystemError("failed to fork");
        }

        if (pid == 0) {
            testWrapper(closures[i], timeoutSecs, std::move(pipes[i].writer), nullptr);
            std::exit(EXIT_SUCCESS);
        }

        pids.push_back(pid);

        if (waitForPython && i == 0)
            waitForPythonReadySigwait(&hEvent, 3);
    }

    std::vector<pollfd> pfds {};

    int timerfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
    if (timerfd < 0) {
        std::for_each(pids.begin(), pids.end(), [](const auto& pid) { kill(pid, SIGKILL); });

        raiseSystemError("failed to create timerfd");
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
            .tv_sec  = timeoutSecs,
            .tv_nsec = 0,
        }};

    if (timerfd_settime(timerfd, 0, &spec, nullptr) < 0) {
        std::for_each(pids.begin(), pids.end(), [](const auto& pid) { kill(pid, SIGKILL); });
        close(timerfd);

        raiseSystemError("failed to set timerfd");
    }

    std::vector<std::optional<TestResult>> results(pids.size(), std::nullopt);

    for (;;) {
        auto n = poll(pfds.data(), pfds.size(), -1);
        if (n < 0) {
            std::for_each(pids.begin(), pids.end(), [](const auto& pid) { kill(pid, SIGKILL); });
            close(timerfd);

            raiseSystemError("failed to poll");
        }

        for (size_t i = 0; i < pfds.size();) {
            auto& pfd = pfds[i];
            if (pfd.revents == 0) {
                i++;
                continue;
            }

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
            auto bytesRead    = read(pfd.fd, &buffer, sizeof(TestResult));
            if (bytesRead == 0) {
                std::cout << "failed to read from pipe: pipe closed unexpectedly\n";
                result = TestResult::Failure;
            } else if (bytesRead < 0) {
                std::cout << "failed to read from pipe: " << std::strerror(errno) << std::endl;
                result = TestResult::Failure;
            } else
                result = (TestResult)buffer;

            // the subprocess should have exited
            // check its exit status
            int status;
            if (waitpid(pids[idx], &status, 0) < 0)
                std::cout << "failed to wait on subprocess[" << idx << "]: " << std::strerror(errno) << std::endl;

            auto exitStatus = WEXITSTATUS(status);
            if (WIFEXITED(status) && exitStatus != EXIT_SUCCESS) {
                std::cout << "subprocess[" << idx << "] exited with status " << exitStatus << std::endl;
            } else if (WIFSIGNALED(status)) {
                std::cout << "subprocess[" << idx << "] killed by signal " << WTERMSIG(status) << std::endl;
            } else {
                std::cout << "subprocess[" << idx << "] completed with "
                          << (result == TestResult::Success ? "Success" : "Failure") << std::endl;
            }

            // store the result
            results[idx] = result;

            // this subprocess is done, remove its pipe from the poll fds
            pfds.erase(pfds.begin() + i);

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
}

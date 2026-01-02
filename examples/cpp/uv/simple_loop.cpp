#include <chrono>
#include <cstdlib>
#include <iostream>
#include <type_traits>

#include "scaler/uv/async.h"
#include "scaler/uv/error.h"
#include "scaler/uv/loop.h"
#include "scaler/uv/signal.h"
#include "scaler/uv/timer.h"

using namespace scaler;

// Simple helper that exits the program when it receives a std::unexpected value.
template <typename T>
static T exitOnFailure(std::expected<T, uv::Error>&& result);

int main()
{
    uv::Loop loop = exitOnFailure(uv::Loop::init());

    std::cout << "Event loop initialized successfully\n";

    // Setting up an Async callback
    uv::Async async = exitOnFailure(uv::Async::init(loop, []() { std::cout << "\tAsync callback executed!\n"; }));
    exitOnFailure(async.send());

    // Setting up a 1 sec. repeating Timer
    uv::Timer timer = exitOnFailure(uv::Timer::init(loop));
    exitOnFailure(timer.start(
        std::chrono::milliseconds(1000),  // Initial delay
        std::chrono::milliseconds(1000),  // Repeat every 1 second
        []() { std::cout << "\tTimer fired\n"; }));

    // Add a Signal handler that stops the loop on Ctrl+C
    uv::Signal signal = exitOnFailure(uv::Signal::init(loop));
    exitOnFailure(signal.start(SIGINT, [&](int signum) {
        std::cout << "\tReceived signal " << signum << ", stopping gracefully...\n";
        loop.stop();
    }));

    std::cout << "Starting event loop...\n";
    std::cout << "Try pressing Ctrl+C to trigger the signal handler\n";

    int activeHandles = loop.run(UV_RUN_DEFAULT);

    std::cout << "Event loop completed with " << activeHandles << " active handles remaining\n";

    return 0;
}

template <typename T>
static T exitOnFailure(std::expected<T, uv::Error>&& result)
{
    if (!result.has_value()) {
        std::cerr << "Operation failed: " + result.error().message() << '\n';
        std::exit(1);
    }

    if constexpr (!std::is_void_v<T>) {
        return std::move(result.value());
    }
}

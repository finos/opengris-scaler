#include <chrono>
#include <cstdlib>
#include <iostream>
#include <type_traits>

#include "scaler/wrapper/uv/async.h"
#include "scaler/wrapper/uv/error.h"
#include "scaler/wrapper/uv/loop.h"
#include "scaler/wrapper/uv/signal.h"
#include "scaler/wrapper/uv/timer.h"
#include "utility.h"  // exitOnFailure

int main()
{
    scaler::wrapper::uv::Loop loop = exitOnFailure(scaler::wrapper::uv::Loop::init());

    std::cout << "Event loop initialized successfully\n";

    // Setting up an Async callback
    scaler::wrapper::uv::Async async =
        exitOnFailure(scaler::wrapper::uv::Async::init(loop, []() { std::cout << "\tAsync callback executed!\n"; }));
    exitOnFailure(async.send());

    // Setting up a 1 sec. repeating Timer
    scaler::wrapper::uv::Timer timer = exitOnFailure(scaler::wrapper::uv::Timer::init(loop));
    exitOnFailure(timer.start(
        std::chrono::milliseconds(1000),  // Initial delay
        std::chrono::milliseconds(1000),  // Repeat every 1 second
        []() { std::cout << "\tTimer fired\n"; }));

    // Add a Signal handler that stops the loop on Ctrl+C
    scaler::wrapper::uv::Signal signal = exitOnFailure(scaler::wrapper::uv::Signal::init(loop));
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

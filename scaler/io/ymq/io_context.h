#pragma once

// C++
#include <memory>
#include <vector>

// First-party
#include "scaler/io/ymq/configuration.h"
#include "scaler/io/ymq/typedefs.h"

class IOSocket;
class EventLoopThread;

class IOContext {
public:
    using Identity               = Configuration::IOSocketIdentity;
    using CreateIOSocketCallback = Configuration::CreateIOSocketCallback;

    IOContext(size_t threadCount = 1) noexcept;
    IOContext(const IOContext&)            = delete;
    IOContext& operator=(const IOContext&) = delete;
    IOContext(IOContext&&)                 = delete;
    IOContext& operator=(IOContext&&)      = delete;

    // These methods need to be thread-safe.
    [[nodiscard("You need this handle to talk to remote end")]]
    std::shared_ptr<IOSocket> createIOSocket(
        Identity identity, IOSocketType socketType, CreateIOSocketCallback onIOSocketCreated) & noexcept;

    // After user called this method, no other call on the passed in IOSocket should be made.
    void removeIOSocket(std::shared_ptr<IOSocket>& socket) noexcept;

    constexpr size_t numThreads() const noexcept { return _threads.size(); }

private:
    std::vector<std::shared_ptr<EventLoopThread>> _threads;
};

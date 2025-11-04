#pragma once

// C++
#include <atomic>
#include <memory>
#include <vector>

// First-party
#include "scaler/ymq/configuration.h"
#include "scaler/ymq/typedefs.h"

namespace scaler {
namespace ymq {

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
    ~IOContext() noexcept;

    void createIOSocket(
        Identity identity, IOSocketType socketType, CreateIOSocketCallback onIOSocketCreated) & noexcept;

    // After user called this method, no other call on the passed in IOSocket should be made.
    void removeIOSocket(std::shared_ptr<IOSocket>& socket) noexcept;

    void requestIOSocketStop(std::shared_ptr<IOSocket> socket) noexcept;

    constexpr size_t numThreads() const noexcept { return _threads.size(); }

private:
    std::vector<std::shared_ptr<EventLoopThread>> _threads;
    std::atomic<size_t> _threadsRoundRobin;
};

}  // namespace ymq
}  // namespace scaler

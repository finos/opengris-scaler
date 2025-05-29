#pragma once

// C++
#include <functional>
#include <memory>

// First-party
#include "event_loop_thread.hpp"
#include "event_manager.hpp"
#include "file_descriptor.hpp"

class TcpServer {
    EventLoopThread& eventLoop;  // eventLoop thread will call onRead that is associated w/ the eventManager
    std::unique_ptr<EventManager> eventManager;  // will copy the `onRead()` to itself
    FileDescriptor fd;
    // Implementation defined method. accept(3) should happen here.
    // This function will call user defined onAcceptReturn()
    // It will handle error it can handle. If it is unreasonable to
    // handle the error here, pass it to onAcceptReturn()
    void onRead();

public:
    TcpServer(const TcpServer&)            = delete;
    TcpServer& operator=(const TcpServer&) = delete;

    using AcceptReturnCallback = std::function<void(FileDescriptor, sockaddr, int)>;
    AcceptReturnCallback onAcceptReturn;
};

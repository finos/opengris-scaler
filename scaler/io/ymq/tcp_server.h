#pragma once

// C++
#include <functional>
#include <memory>

// First-party
#include "scaler/io/ymq/event_loop_thread.h"
#include "scaler/io/ymq/event_manager.h"
#include "scaler/io/ymq/file_descriptor.h"

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

#pragma once

// C++
#include <functional>
#include <memory>

// First-party
#include "scaler/io/ymq/file_descriptor.h"
// #include "event_loop_thread.hpp"
// #include "event_manager.hpp"

class EventLoopThread;
class EventManager;

class TcpServer {
    // eventLoop thread will call onRead that is associated w/ the eventManager
    std::shared_ptr<EventLoopThread> eventLoop;
    std::unique_ptr<EventManager> eventManager;  // will copy the `onRead()` to itself
    int serverFd;
    // Implementation defined method. accept(3) should happen here.
    // This function will call user defined onAcceptReturn()
    // It will handle error it can handle. If it is unreasonable to
    // handle the error here, pass it to onAcceptReturn()
    void onRead();

public:
    TcpServer(const TcpServer&)            = delete;
    TcpServer& operator=(const TcpServer&) = delete;

    // TODO: Modify the behavior of default ctor
    TcpServer(std::shared_ptr<EventLoopThread> eventLoop);

    using AcceptReturnCallback = std::function<void(FileDescriptor, sockaddr, int)>;
    AcceptReturnCallback onAcceptReturn;
    void onCreated();
};

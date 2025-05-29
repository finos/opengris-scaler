#pragma once

// C++
#include <functional>
#include <memory>

// First-party
#include "event_loop_thread.hpp"
#include "event_manager.hpp"
#include "file_descriptor.hpp"

class TcpClient {
    EventLoopThread& eventLoop; /* shared ownership */
    std::unique_ptr<EventManager> eventManager;
    // Implementation defined method. connect(3) should happen here.
    // This function will call user defined onConnectReturn()
    // It will handle error it can handle. If it is unreasonable to
    // handle the error here, pass it to onConnectReturn()
    void onWrite();

public:
    TcpClient(const TcpClient&)            = delete;
    TcpClient& operator=(const TcpClient&) = delete;

    using ConnectReturnCallback = std::function<void(FileDescriptor, sockaddr, int)>;
    ConnectReturnCallback onConnectReturn;

    void retry(/* Arguments */);
};

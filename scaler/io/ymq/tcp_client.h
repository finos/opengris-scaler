#pragma once

// C++
#include <functional>
#include <memory>

// First-party
#include "scaler/io/ymq/event_loop_thread.h"
#include "scaler/io/ymq/event_manager.h"
#include "scaler/io/ymq/file_descriptor.h"

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

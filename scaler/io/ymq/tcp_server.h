#pragma once

// C++
#include <sys/socket.h>

#include <memory>

// First-party
#include "scaler/io/ymq/configuration.h"
#include "scaler/io/ymq/file_descriptor.h"

class EventLoopThread;
class EventManager;

class TcpServer {
    // eventLoop thread will call onRead that is associated w/ the eventManager
    std::shared_ptr<EventLoopThread> _eventLoopThread;
    std::unique_ptr<EventManager> _eventManager;  // will copy the `onRead()` to itself
    int _serverFd;

    // Implementation defined method. accept(3) should happen here.
    // This function will call user defined onAcceptReturn()
    // It will handle error it can handle. If it is unreasonable to
    // handle the error here, pass it to onAcceptReturn()
    void onRead();
    void onWrite() {}
    void onClose() {}
    void onError() {}

    sockaddr _addr;
    std::string _localIOSocketIdentity;

public:
    using BindReturnCallback = Configuration::BindReturnCallback;
    BindReturnCallback _onBindReturn;

    TcpServer(const TcpServer&)            = delete;
    TcpServer& operator=(const TcpServer&) = delete;

    TcpServer(
        std::shared_ptr<EventLoopThread> eventLoop,
        std::string localIOSocketIdentity,
        sockaddr addr,
        BindReturnCallback onBindReturn);

    void onCreated();
    ~TcpServer();
};

#pragma once

// C++
#ifdef __linux__
#include <sys/socket.h>
#endif  // __linux__
#ifdef _WIN32
// clang-format off
#include <winsock2.h>
#include <mswsock.h>
// clang-format on
#endif  // _WIN32

#include <memory>

// First-party
#include "scaler/ymq/configuration.h"
#include "scaler/logging/logging.h"

namespace scaler {
namespace ymq {

class EventLoopThread;
class EventManager;

class TcpServer {
public:
    using BindReturnCallback = Configuration::BindReturnCallback;

    TcpServer(
        std::shared_ptr<EventLoopThread> eventLoop,
        std::string localIOSocketIdentity,
        sockaddr addr,
        BindReturnCallback onBindReturn) noexcept;
    TcpServer(const TcpServer&)            = delete;
    TcpServer& operator=(const TcpServer&) = delete;
    ~TcpServer() noexcept;

    void onCreated();
    std::shared_ptr<EventLoopThread> _eventLoopThread;

private:
    // Implementation defined method. accept(3) should happen here.
    // This function will call user defined onAcceptReturn()
    // It will handle error it can handle. If it is unreasonable to
    // handle the error here, pass it to onAcceptReturn()
    void onRead();
    void onWrite() {}
    void onClose() { printf("%s\n", __PRETTY_FUNCTION__); }
    void onError() { printf("%s\n", __PRETTY_FUNCTION__); }

    int createAndBindSocket();

    BindReturnCallback _onBindReturn;
    // Because here we need to pass the sizeof _serverFd to setsockopt.
#ifdef _WIN32
    SOCKET _serverFd;
#endif
#ifdef __linux__
    int _serverFd;
#endif
    sockaddr _addr;
    std::string _localIOSocketIdentity;

    std::unique_ptr<EventManager> _eventManager;  // will copy the `onRead()` to itself

    Logger _logger;

#ifdef _WIN32
    // We break the decl rule here because otherwise the implementation would be messy.
    LPFN_ACCEPTEX _acceptExFunc;
    SOCKET _newConn;
    char _buffer[128];
    void prepareAcceptSocket();
#endif  // _WIN32
};

}  // namespace ymq
}  // namespace scaler

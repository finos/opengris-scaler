#pragma once

#include <memory>

#include "scaler/ymq/internal/raw_server_tcp_fd.h"

// First-party
#include "scaler/logging/logging.h"
#include "scaler/ymq/configuration.h"

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

    bool createAndBindSocket();

    BindReturnCallback _onBindReturn;
    sockaddr _addr;
    std::string _localIOSocketIdentity;

    std::unique_ptr<EventManager> _eventManager;  // will copy the `onRead()` to itself

    Logger _logger;

    RawServerTCPFD _rawServer;
};

}  // namespace ymq
}  // namespace scaler

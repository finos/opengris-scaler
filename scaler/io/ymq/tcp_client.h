#pragma once

#ifdef __linux__
#include <sys/socket.h>
#endif  // __linux__
#ifdef _WIN32
// clang-format off
#include <windows.h>
#include <winsock2.h>
#include <mswsock.h>
// clang-format on
#endif  // _WIN32

#include <memory>

// First-party
#include "scaler/io/ymq/configuration.h"

namespace scaler {
namespace ymq {

class EventLoopThread;
class EventManager;

class TcpClient {
public:
    using ConnectReturnCallback = Configuration::ConnectReturnCallback;

    TcpClient(
        std::shared_ptr<EventLoopThread> eventLoopThread,
        std::string localIOSocketIdentity,
        sockaddr remoteAddr,
        ConnectReturnCallback onConnectReturn,
        size_t maxRetryTimes) noexcept;
    TcpClient(const TcpClient&)            = delete;
    TcpClient& operator=(const TcpClient&) = delete;
    ~TcpClient() noexcept;

    void onCreated();
    void retry();

    std::shared_ptr<EventLoopThread> _eventLoopThread; /* shared ownership */
    bool _connected;

private:
    // Implementation defined method. connect(3) should happen here.
    // This function will call user defined onConnectReturn()
    // It will handle error it can handle. If it is unreasonable to
    // handle the error here, pass it to onConnectReturn()
    void onRead();
    void onWrite();
    void onClose() {}
    void onError() {}

    ConnectReturnCallback _onConnectReturn;
    int _connFd;
    std::string _localIOSocketIdentity;
    sockaddr _remoteAddr;
    int _retryIdentifier;

    std::unique_ptr<EventManager> _eventManager;
    size_t _retryTimes;

    const size_t _maxRetryTimes;

#ifdef _WIN32
    LPFN_CONNECTEX _connectExFunc;
#endif  // _WIN32
};

}  // namespace ymq
}  // namespace scaler

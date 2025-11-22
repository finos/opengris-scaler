#pragma once

#include <memory>

// First-party
#include "scaler/logging/logging.h"
#include "scaler/ymq/configuration.h"
#include "scaler/ymq/internal/raw_client_tcp_fd.h"

namespace scaler {
namespace ymq {

class EventLoopThread;
class EventManager;

class TCPClient {
public:
    using ConnectReturnCallback = Configuration::ConnectReturnCallback;

    TCPClient(
        EventLoopThread* eventLoopThread,
        std::string localIOSocketIdentity,
        sockaddr remoteAddr,
        ConnectReturnCallback onConnectReturn,
        size_t maxRetryTimes) noexcept;
    TCPClient(const TCPClient&)            = delete;
    TCPClient& operator=(const TCPClient&) = delete;
    ~TCPClient() noexcept;

    void onCreated();
    void retry();
    void disconnect();

    EventLoopThread* _eventLoopThread; /* shared ownership */
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
    std::string _localIOSocketIdentity;
    int _retryIdentifier;

    Logger _logger;

    std::unique_ptr<EventManager> _eventManager;
    size_t _retryTimes;

    const size_t _maxRetryTimes;

    RawClientTCPFD _rawClient;
};

}  // namespace ymq
}  // namespace scaler

#pragma once

#include <memory>

// First-party
#include "scaler/logging/logging.h"
#include "scaler/ymq/configuration.h"
#include "scaler/ymq/internal/raw_stream_client_handle.h"

namespace scaler {
namespace ymq {

class EventLoopThread;
class EventManager;

class StreamClient {
public:
    using ConnectReturnCallback = Configuration::ConnectReturnCallback;

    StreamClient(
        EventLoopThread* eventLoopThread,
        std::string localIOSocketIdentity,
        sockaddr remoteAddr,
        ConnectReturnCallback onConnectReturn,
        size_t maxRetryTimes) noexcept;
    StreamClient(const StreamClient&)            = delete;
    StreamClient& operator=(const StreamClient&) = delete;
    ~StreamClient() noexcept;

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

    RawStreamClientHandle _rawClient;
};

}  // namespace ymq
}  // namespace scaler

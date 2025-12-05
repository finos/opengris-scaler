#pragma once

#include <memory>

#include "scaler/ymq/internal/raw_stream_server_handle.h"
#include "scaler/ymq/internal/socket_address.h"

// First-party
#include "scaler/logging/logging.h"
#include "scaler/ymq/configuration.h"
#include "scaler/ymq/internal/socket_address.h"

struct sockaddr;

namespace scaler {
namespace ymq {

class EventLoopThread;
class EventManager;

class StreamServer {
public:
    using BindReturnCallback = Configuration::BindReturnCallback;

    StreamServer(
        EventLoopThread* eventLoop,
        std::string localIOSocketIdentity,
        SocketAddress addr,
        BindReturnCallback onBindReturn) noexcept;
    StreamServer(const StreamServer&)            = delete;
    StreamServer& operator=(const StreamServer&) = delete;
    ~StreamServer() noexcept;

    void disconnect();
    void onCreated();
    EventLoopThread* _eventLoopThread;

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
    std::string _localIOSocketIdentity;

    std::unique_ptr<EventManager> _eventManager;  // will copy the `onRead()` to itself

    Logger _logger;

    RawStreamServerHandle _rawServer;
};

}  // namespace ymq
}  // namespace scaler

#include "scaler/ymq/tcp_client.h"

#include <chrono>
#include <memory>

#include "scaler/error/error.h"
#include "scaler/ymq/event_loop_thread.h"
#include "scaler/ymq/event_manager.h"
#include "scaler/ymq/io_socket.h"
#include "scaler/ymq/message_connection_tcp.h"
#include "scaler/ymq/network_utils.h"
#include "scaler/ymq/timestamp.h"

namespace scaler {
namespace ymq {

void TCPClient::onCreated()
{
    assert(_rawClient.nativeHandle() == 0);
    assert(_eventManager.get() != nullptr);
    _rawClient.create();
    _eventLoopThread->_eventLoop.addFdToLoop(_rawClient.nativeHandle(), EPOLLOUT | EPOLLET, this->_eventManager.get());
    if (_rawClient.prepConnect(this->_eventManager.get())) [[unlikely]] {
        _eventLoopThread->_eventLoop.removeFdFromLoop(_rawClient.nativeHandle());
        std::string id                 = this->_localIOSocketIdentity;
        auto sock                      = this->_eventLoopThread->_identityToIOSocket.at(id);
        const bool responsibleForRetry = true;
        sock->onConnectionCreated(
            setNoDelay(_rawClient.nativeHandle()),
            getLocalAddr(_rawClient.nativeHandle()),
            getRemoteAddr(_rawClient.nativeHandle()),
            responsibleForRetry);
        if (_retryTimes == 0) {
            _onConnectReturn({});
            _onConnectReturn = {};
        }
        return;
    } else {
        if (_retryTimes == 0) {
            _onConnectReturn(std::unexpected {Error::ErrorCode::InitialConnectFailedWithInProgress});
            _onConnectReturn = {};
        }
        return;
    }
}

TCPClient::TCPClient(
    EventLoopThread* eventLoopThread,
    std::string localIOSocketIdentity,
    sockaddr remoteAddr,
    ConnectReturnCallback onConnectReturn,
    size_t maxRetryTimes) noexcept
    : _eventLoopThread(eventLoopThread)
    , _connected(false)
    , _onConnectReturn(std::move(onConnectReturn))
    , _localIOSocketIdentity(std::move(localIOSocketIdentity))
    , _retryIdentifier {}
    , _eventManager(std::make_unique<EventManager>())
    , _retryTimes {}
    , _maxRetryTimes(maxRetryTimes)
    , _rawClient(std::move(remoteAddr))
{
    _eventManager->onRead  = [this] { this->onRead(); };
    _eventManager->onWrite = [this] { this->onWrite(); };
    _eventManager->onClose = [this] { this->onClose(); };
    _eventManager->onError = [this] { this->onError(); };
}

void TCPClient::onRead()
{
}

void TCPClient::onWrite()
{
    if (!_rawClient.nativeHandle()) {
        return;
    }

    if (_rawClient.needRetry()) {
        _rawClient.destroy();
        retry();
        return;
    }

    std::string id                 = this->_localIOSocketIdentity;
    auto sock                      = this->_eventLoopThread->_identityToIOSocket.at(id);
    const bool responsibleForRetry = true;
    sock->onConnectionCreated(
        setNoDelay(_rawClient.nativeHandle()),
        getLocalAddr(_rawClient.nativeHandle()),
        getRemoteAddr(_rawClient.nativeHandle()),
        responsibleForRetry);

    _rawClient.zeroNativeHandle();
    _connected = true;

    _eventLoopThread->_eventLoop.executeLater([sock] { sock->removeConnectedTCPClient(); });
}

void TCPClient::retry()
{
    if (_retryTimes > _maxRetryTimes) {
        _logger.log(Logger::LoggingLevel::error, "Retried times has reached maximum: ", _maxRetryTimes);
        // exit(1);
        return;
    }

    _logger.log(Logger::LoggingLevel::debug, "Client retrying ", _retryTimes, " time(s)");

    Timestamp now;
    auto at = now.createTimestampByOffsetDuration(std::chrono::seconds(2 << _retryTimes++));

    _retryIdentifier = _eventLoopThread->_eventLoop.executeAt(at, [this] { this->onCreated(); });
}

void TCPClient::disconnect()
{
    if (_rawClient.nativeHandle()) {
        _eventLoopThread->_eventLoop.removeFdFromLoop(_rawClient.nativeHandle());
        _rawClient.destroy();
    }
}

TCPClient::~TCPClient() noexcept
{
    disconnect();
    if (_retryTimes > 0) {
        _eventLoopThread->_eventLoop.cancelExecution(_retryIdentifier);
    }
    // TODO: Do we think this is an error? See TCPServer::~TCPServer for detail.
    if (_onConnectReturn) {
        _onConnectReturn({});
    }
}

}  // namespace ymq
}  // namespace scaler

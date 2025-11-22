#include "scaler/ymq/tcp_server.h"

#include <expected>
#include <memory>

#include "scaler/error/error.h"
#include "scaler/ymq/event_loop_thread.h"
#include "scaler/ymq/event_manager.h"
#include "scaler/ymq/io_socket.h"
#include "scaler/ymq/message_connection_tcp.h"
#include "scaler/ymq/network_utils.h"

namespace scaler {
namespace ymq {

bool TCPServer::createAndBindSocket()
{
    if (!_rawServer.setReuseAddress()) {
        _logger.log(
            Logger::LoggingLevel::error,
            "Originated from",
            "setsockopt(2)",
            "Errno is",
            strerror(GetErrorCode())  // ,
        );
        _onBindReturn(std::unexpected(Error {Error::ErrorCode::SetSockOptNonFatalFailure}));
        _onBindReturn = {};
        return false;
    }

    _rawServer.bindAndListen();

    return true;
}

TCPServer::TCPServer(
    EventLoopThread* eventLoopThread,
    std::string localIOSocketIdentity,
    sockaddr addr,
    BindReturnCallback onBindReturn) noexcept
    : _eventLoopThread(eventLoopThread)
    , _onBindReturn(std::move(onBindReturn))
    , _addr(addr)
    , _localIOSocketIdentity(std::move(localIOSocketIdentity))
    , _eventManager(std::make_unique<EventManager>())
    , _rawServer(std::move(addr))
{
    _eventManager->onRead  = [this] { this->onRead(); };
    _eventManager->onWrite = [this] { this->onWrite(); };
    _eventManager->onClose = [this] { this->onClose(); };
    _eventManager->onError = [this] { this->onError(); };
}

void TCPServer::onCreated()
{
    if (!createAndBindSocket()) {
        return;
    }
    _eventLoopThread->_eventLoop.addFdToLoop(_rawServer.nativeHandle(), EPOLLIN | EPOLLET, this->_eventManager.get());

    _rawServer.prepareAcceptSocket((void*)_eventManager.get());

    _onBindReturn({});
    _onBindReturn = {};
}

void TCPServer::disconnect()
{
    if (_rawServer.nativeHandle()) {
        _eventLoopThread->_eventLoop.removeFdFromLoop(_rawServer.nativeHandle());
        _rawServer.destroy();
    }
}

void TCPServer::onRead()
{
    if (!_rawServer.nativeHandle()) {
        return;
    }

    const auto& id = this->_localIOSocketIdentity;
    auto sock      = this->_eventLoopThread->_identityToIOSocket.at(id);

    auto fdAndRemoteAddrs = _rawServer.getNewConns();

    for (const auto& fdAndRemoteAddr: fdAndRemoteAddrs) {
        sock->onConnectionCreated(
            setNoDelay(fdAndRemoteAddr.first), getLocalAddr(fdAndRemoteAddr.first), fdAndRemoteAddr.second, false);
    }

    _rawServer.prepareAcceptSocket((void*)_eventManager.get());
}

TCPServer::~TCPServer() noexcept
{
    disconnect();
    // TODO: Do we think this is an error? In extreme cases:
    // bindTo(...);
    // removeIOSocket(...);
    // Below callback may not be called.
    if (_onBindReturn) {
        _onBindReturn({});
    }
}

}  // namespace ymq
}  // namespace scaler

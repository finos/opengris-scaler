#include "scaler/ymq/io_socket.h"

#include <algorithm>
#include <expected>
#include <memory>
#include <optional>
#include <ranges>
#include <utility>
#include <vector>

#include "scaler/error/error.h"
#include "scaler/ymq/event_loop_thread.h"
#include "scaler/ymq/event_manager.h"
#include "scaler/ymq/internal/network_utils.h"
#include "scaler/ymq/internal/raw_stream_connection_handle.h"
#include "scaler/ymq/internal/socket_address.h"
#include "scaler/ymq/message_connection.h"
#include "scaler/ymq/stream_client.h"
#include "scaler/ymq/stream_server.h"
#include "scaler/ymq/typedefs.h"

namespace scaler {
namespace ymq {

IOSocket::IOSocket(
    std::shared_ptr<EventLoopThread> eventLoopThread, Identity identity, IOSocketType socketType) noexcept
    : _eventLoopThread(eventLoopThread)
    , _identity(std::move(identity))
    , _socketType(std::move(socketType))
    , _pendingRecvMessages(std::queue<RecvMessageCallback>())
    , _leftoverMessagesAfterConnectionDied(std::queue<Message>())
    , _stopped {false}
    , _connectorDisconnected {false}
{
}

void IOSocket::sendMessage(Message message, SendMessageCallback onMessageSent) noexcept
{
    _eventLoopThread->_eventLoop.executeNow(
        [this, message = std::move(message), callback = std::move(onMessageSent)] mutable {
            if (_stopped) {
                callback(std::unexpected {Error::ErrorCode::IOSocketStopRequested});
                return;
            }

            std::string address = std::string((char*)message.address.data(), message.address.len());

            // Preparation and early out
            switch (socketType()) {
                case IOSocketType::Binder: {
                    if (!message.address.data()) {
                        callback(std::unexpected {Error::ErrorCode::BinderSendMessageWithNoAddress});
                        return;
                    }
                    break;
                }
                case IOSocketType::Connector: {
                    if (_connectorDisconnected) {
                        callback(std::unexpected {Error::ErrorCode::ConnectorSocketClosedByRemoteEnd});
                        return;
                    }
                    address = "";
                    break;
                }
                case IOSocketType::Multicast: {
                    callback({});  // SUCCESS
                    for (const auto& [addr, conn]: _identityToConnection) {
                        // TODO: Currently doing N copies of the messages. Find a place to
                        // store this message and pass in reference.
                        if (addr.starts_with(address))
                            conn->sendMessage(message, [](auto) {});
                    }
                    return;
                }

                case IOSocketType::Uninit:
                case IOSocketType::Unicast:
                default: break;
            }

            MessageConnection* conn = nullptr;

            if (this->_identityToConnection.contains(address)) {
                conn = this->_identityToConnection[address].get();
            } else {
                const auto it =
                    std::ranges::find(_unestablishedConnection, address, &MessageConnection::_remoteIOSocketIdentity);
                if (it != _unestablishedConnection.end()) {
                    conn = it->get();
                } else {
                    onConnectionCreated(address);
                    conn = _unestablishedConnection.back().get();
                }
            }
            conn->sendMessage(std::move(message), std::move(callback));
        });
}

void IOSocket::recvMessage(RecvMessageCallback onRecvMessage) noexcept
{
    _eventLoopThread->_eventLoop.executeNow([this, callback = std::move(onRecvMessage)] mutable {
        if (_stopped) {
            callback({{}, Error::ErrorCode::IOSocketStopRequested});
            return;
        }

        if (_leftoverMessagesAfterConnectionDied.size()) {
            auto msg = std::move(_leftoverMessagesAfterConnectionDied.front());
            _leftoverMessagesAfterConnectionDied.pop();
            callback({std::move(msg), Error::ErrorCode::Uninit});
            return;
        }

        if (_connectorDisconnected) {
            callback({{}, Error::ErrorCode::ConnectorSocketClosedByRemoteEnd});
            return;
        }

        this->_pendingRecvMessages.emplace(std::move(callback));
        if (_pendingRecvMessages.size() == 1) {
            for (const auto& [fd, conn]: _identityToConnection) {
                if (conn->recvMessage())
                    return;
            }
        }
    });
}

void IOSocket::connectTo(SocketAddress addr, ConnectReturnCallback onConnectReturn, size_t maxRetryTimes) noexcept
{
    _eventLoopThread->_eventLoop.executeNow(
        [this, addr = std::move(addr), callback = std::move(onConnectReturn), maxRetryTimes] mutable {
            if (addr.nativeHandleType() == SocketAddress::Type::TCP) {
                if (_tcpClient) {
                    unrecoverableError({
                        Error::ErrorCode::MultipleConnectToNotSupported,
                        "Originated from",
                        "IOSocket::connectTo",
                    });
                }

                _tcpClient.emplace(
                    _eventLoopThread.get(), this->identity(), std::move(addr), std::move(callback), maxRetryTimes);
                _tcpClient->onCreated();

            } else if (addr.nativeHandleType() == SocketAddress::Type::IPC) {
                if (_ipcClient) {
                    unrecoverableError({
                        Error::ErrorCode::MultipleConnectToNotSupported,
                        "Originated from",
                        "IOSocket::connectTo",
                    });
                }

                _ipcClient.emplace(
                    _eventLoopThread.get(), this->identity(), std::move(addr), std::move(callback), maxRetryTimes);
                _ipcClient->onCreated();

            } else {
                std::unreachable();  // current protocol supports only tcp and icp
            }
        });
}

void IOSocket::connectTo(
    std::string netOrDomainAddr, ConnectReturnCallback onConnectReturn, size_t maxRetryTimes) noexcept
{
    auto socketAddressResult = stringToSocketAddress(netOrDomainAddr);
    if (!socketAddressResult) {
        onConnectReturn(std::unexpected {socketAddressResult.error()});
        return;
    }
    connectTo(std::move(socketAddressResult.value()), std::move(onConnectReturn), maxRetryTimes);
}

void IOSocket::bindTo(std::string netOrDomainAddr, BindReturnCallback onBindReturn) noexcept
{
    _eventLoopThread->_eventLoop.executeNow([this,
                                             netOrDomainAddr = std::move(netOrDomainAddr),
                                             callback        = std::move(onBindReturn)] mutable {
        assert(netOrDomainAddr.size());
        auto socketAddressResult = stringToSocketAddress(netOrDomainAddr);
        if (!socketAddressResult) {
            callback(std::unexpected {socketAddressResult.error()});
            return;
        }
        const auto& socketAddress = socketAddressResult.value();

        if (socketAddress.nativeHandleType() == SocketAddress::Type::TCP) {
            if (_tcpServer) {
                callback(std::unexpected {Error::ErrorCode::MultipleBindToNotSupported});
                return;
            }

            _tcpServer.emplace(_eventLoopThread.get(), this->identity(), std::move(socketAddress), std::move(callback));
            _tcpServer->onCreated();

        } else if (socketAddress.nativeHandleType() == SocketAddress::Type::IPC) {
            if (_ipcServer) {
                callback(std::unexpected {Error::ErrorCode::MultipleBindToNotSupported});
                return;
            }

            _ipcServer.emplace(_eventLoopThread.get(), this->identity(), std::move(socketAddress), std::move(callback));
            _ipcServer->onCreated();

        } else {
            std::unreachable();  // current protocol supports only tcp and icp
        }
    });
}

void IOSocket::closeConnection(Identity remoteSocketIdentity) noexcept
{
    _eventLoopThread->_eventLoop.executeNow([this, remoteIdentity = std::move(remoteSocketIdentity)] {
        if (_stopped) {
            return;
        }
        if (_identityToConnection.contains(remoteIdentity))
            _identityToConnection[remoteIdentity]->disconnect();
    });
}

void IOSocket::onConnectorMaxedOutRetry() noexcept
{
    assert(_unestablishedConnection.size());
    assert(IOSocketType::Connector == this->_socketType);
    _connectorDisconnected = true;
    fillPendingRecvMessagesWithErr(Error::ErrorCode::ConnectorSocketClosedByRemoteEnd);
    auto& connPtr = _unestablishedConnection.back();
    _eventLoopThread->_eventLoop.executeLater([conn = std::move(connPtr)]() {});
    _unestablishedConnection.pop_back();
}

void IOSocket::onConnectionDisconnected(MessageConnection* conn, bool keepInBook) noexcept
{
    if (!conn->_remoteIOSocketIdentity) {
        if (IOSocketType::Connector == this->_socketType) {
            _connectorDisconnected = true;
            fillPendingRecvMessagesWithErr(Error::ErrorCode::ConnectorSocketClosedByRemoteEnd);
        }
        auto connIt = std::ranges::find_if(_unestablishedConnection, [&](const auto& x) { return x.get() == conn; });
        assert(connIt != _unestablishedConnection.end());
        _eventLoopThread->_eventLoop.executeLater([conn = std::move(*connIt)] {});
        _unestablishedConnection.erase(connIt);
        return;
    }

    auto connIt = this->_identityToConnection.find(*conn->_remoteIOSocketIdentity);

    _unestablishedConnection.push_back(std::move(connIt->second));
    this->_identityToConnection.erase(connIt);
    auto& connPtr = _unestablishedConnection.back();

    if (!keepInBook) {
        if (IOSocketType::Connector == this->_socketType) {
            _connectorDisconnected = true;
            fillPendingRecvMessagesWithErr(Error::ErrorCode::ConnectorSocketClosedByRemoteEnd);
        }
        _eventLoopThread->_eventLoop.executeLater([conn = std::move(connPtr)]() {});
        _unestablishedConnection.pop_back();
        return;
    }

    if (socketType() == IOSocketType::Unicast || socketType() == IOSocketType::Multicast) {
        auto destructWriteOp = std::move(connPtr->_writeOperations);
        connPtr->_writeOperations.clear();
        fillPendingRecvMessagesWithErr(Error::ErrorCode::RemoteEndDisconnectedOnSocketWithoutGuaranteedDelivery);
        auto destructReadOp = std::move(connPtr->_receivedReadOperations);
    }

    if (connPtr->_responsibleForRetry) {
        connectTo(connPtr->_remoteAddr, [](auto) {});  // as the user callback is one-shot
    }
}

// FIXME: This algorithm runs in O(n) complexity. To reduce the complexity of this algorithm
// to O(lg n), one has to restructure how connections are placed. We would have three lists:
// - _unconnectedConnections that holds connections with identity but not fd
// - _unestablishedConnections that holds connections with fd but not identity
// - _connectingConnections that holds connections with fd and identity
// And this three lists shall be lookedup in above order based on this rule:
// - look up in _unestablishedConnections and move this connection to  _connectingConnections
// - look up _unconnectedConnections to find if there's a connection with the same identity
//   if so, merge it to this connection that currently resides in _connectingConnections
// Similar thing for disconnection as well.
void IOSocket::onConnectionIdentityReceived(MessageConnection* conn) noexcept
{
    auto& s = conn->_remoteIOSocketIdentity;
    if (socketType() == IOSocketType::Connector) {
        s = "";
    }

    auto thisConn = std::find_if(_unestablishedConnection.begin(), _unestablishedConnection.end(), [&](const auto& x) {
        return x.get() == conn;
    });
    _identityToConnection[*s] = std::move(*thisConn);
    _unestablishedConnection.erase(thisConn);

    auto rge = _unestablishedConnection                                                                        //
               | std::views::filter([](const auto& x) { return x->_remoteIOSocketIdentity != std::nullopt; })  //
               | std::views::filter([&s](const auto& x) { return *x->_remoteIOSocketIdentity == *s; })         //
               | std::views::take(1);
    if (rge.empty()) {
        return;
    }

    auto& targetConn = _identityToConnection[*s];

    auto c = _unestablishedConnection.begin() + (_unestablishedConnection.size() - rge.begin().count());

    while ((*c)->_writeOperations.size()) {
        targetConn->_writeOperations.emplace_back(std::move((*c)->_writeOperations.front()));
        (*c)->_writeOperations.pop_front();
    }

    targetConn->_pendingRecvMessageCallbacks = std::move((*c)->_pendingRecvMessageCallbacks);

    assert(targetConn->_receivedReadOperations.empty());
    targetConn->_receivedReadOperations = std::move((*c)->_receivedReadOperations);

    assert((*c)->_rawConn.nativeHandle() == 0);
    _unestablishedConnection.erase(c);
}

void IOSocket::onConnectionCreated(std::string remoteIOSocketIdentity) noexcept
{
    _unestablishedConnection.push_back(
        std::make_unique<MessageConnection>(
            _eventLoopThread.get(),
            this->identity(),
            std::move(remoteIOSocketIdentity),
            &_pendingRecvMessages,
            &_leftoverMessagesAfterConnectionDied));
    _unestablishedConnection.back()->onCreated();
}

void IOSocket::onConnectionCreated(
    int fd, SocketAddress localAddr, SocketAddress remoteAddr, bool responsibleForRetry) noexcept
{
    _unestablishedConnection.push_back(
        std::make_unique<MessageConnection>(
            _eventLoopThread.get(),
            fd,
            std::move(localAddr),
            std::move(remoteAddr),
            this->identity(),
            responsibleForRetry,
            &_pendingRecvMessages,
            &_leftoverMessagesAfterConnectionDied));
    _unestablishedConnection.back()->onCreated();
}

void IOSocket::removeConnectedStreamClient(const StreamClient* client) noexcept
{
    if (this->_tcpClient && &(*this->_tcpClient) == client) {
        this->_tcpClient.reset();
    }
    if (this->_ipcClient && &(*this->_ipcClient) == client) {
        this->_ipcClient.reset();
    }
}

void IOSocket::requestStop() noexcept
{
    _stopped = true;
    fillPendingRecvMessagesWithErr(Error::ErrorCode::IOSocketStopRequested);

    std::ranges::for_each(_identityToConnection, [](const auto& x) { x.second->disconnect(); });
    std::ranges::for_each(_unestablishedConnection, [](const auto& x) { x->disconnect(); });

    if (_tcpServer) {
        _tcpServer->disconnect();
    }
    if (_tcpClient) {
        _tcpClient->disconnect();
    }

    if (_ipcClient) {
        _ipcClient->disconnect();
    }
    if (_ipcServer) {
        _ipcServer->disconnect();
    }
}

size_t IOSocket::numOfConnections()
{
    auto connectedConnections =
        std::ranges::count_if(_unestablishedConnection, [](const auto& x) { return !x->disconnected(); });
    connectedConnections += _identityToConnection.size();
    return connectedConnections;
}

void IOSocket::fillPendingRecvMessagesWithErr(Error err)
{
    while (_pendingRecvMessages.size()) {
        auto readOp = std::move(_pendingRecvMessages.front());
        _pendingRecvMessages.pop();
        readOp({{}, err});
    }
}

IOSocket::~IOSocket() noexcept
{
    fillPendingRecvMessagesWithErr(Error::ErrorCode::IOSocketStopRequested);
}

}  // namespace ymq
}  // namespace scaler

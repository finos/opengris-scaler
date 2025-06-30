#include "scaler/io/ymq/io_socket.h"

#include <algorithm>
#include <cstdint>
#include <expected>
#include <memory>
#include <ranges>
#include <utility>
#include <vector>

#include "scaler/io/ymq/event_loop_thread.h"
#include "scaler/io/ymq/event_manager.h"
#include "scaler/io/ymq/message_connection_tcp.h"
#include "scaler/io/ymq/tcp_client.h"
#include "scaler/io/ymq/tcp_server.h"
#include "scaler/io/ymq/utils.h"

void IOSocket::removeConnectedTcpClient() {
    if (this->_tcpClient && this->_tcpClient->_connected) {
        this->_tcpClient.reset();
    }
}

IOSocket::IOSocket(std::shared_ptr<EventLoopThread> eventLoopThread, Identity identity, IOSocketType socketType)
    : _eventLoopThread(eventLoopThread)
    , _identity(std::move(identity))
    , _socketType(std::move(socketType))
    , _pendingReadOperations(std::make_shared<std::queue<TcpReadOperation>>()) {}

void IOSocket::connectTo(sockaddr addr, ConnectReturnCallback callback) {
    _eventLoopThread->_eventLoop.executeNow([this, addr = std::move(addr), callback = std::move(callback)] {
        _tcpClient.emplace(_eventLoopThread, this->identity(), std::move(addr), std::move(callback));
        _tcpClient->onCreated();
    });
}

void IOSocket::connectTo(std::string networkAddress, ConnectReturnCallback callback) {
    auto res = stringToSockaddr(std::move(networkAddress));
    assert(res);
    connectTo(std::move(res.value()), std::move(callback));
}

void IOSocket::bindTo(std::string networkAddress, BindReturnCallback callback) {
    _eventLoopThread->_eventLoop.executeNow(
        [this, networkAddress = std::move(networkAddress), callback = std::move(callback)] {
            if (_tcpServer) {
                callback(-1);
                return;
            }
            auto res = stringToSockaddr(std::move(networkAddress));
            assert(res);

            _tcpServer.emplace(_eventLoopThread, this->identity(), std::move(res.value()), std::move(callback));
            _tcpServer->onCreated();
        });
}

void IOSocket::onConnectionDisconnected(MessageConnectionTCP* conn) {
    if (!conn->_remoteIOSocketIdentity) {
        return;
    }

    auto connIt = this->_identityToConnection.find(*conn->_remoteIOSocketIdentity);
    _unestablishedConnection.push_back(std::move(connIt->second));
    this->_identityToConnection.erase(connIt);

    auto& connPtr = _unestablishedConnection.back();
    if (connPtr->_responsibleForRetry) {
        connectTo(connPtr->_remoteAddr, [](int) {});  // as the user callback is one-shot
    }
}

void IOSocket::onConnectionIdentityReceived(MessageConnectionTCP* conn) {
    const auto& s = conn->_remoteIOSocketIdentity;
    auto thisConn = std::find_if(_unestablishedConnection.begin(), _unestablishedConnection.end(), [&](const auto& x) {
        return x.get() == conn;
    });
    _identityToConnection[*s] = std::move(*thisConn);
    _unestablishedConnection.erase(thisConn);

    auto c = std::find_if(_unestablishedConnection.begin(), _unestablishedConnection.end(), [&](const auto& x) {
        return *s == *x->_remoteIOSocketIdentity;
    });

    if (c == _unestablishedConnection.end())
        return;

    (_identityToConnection[*s])->_writeOperations       = std::move((*c)->_writeOperations);
    (_identityToConnection[*s])->_pendingReadOperations = std::move((*c)->_pendingReadOperations);
    (_identityToConnection[*s])->_receivedMessages      = std::move((*c)->_receivedMessages);
    _unestablishedConnection.erase(c);
}

void IOSocket::sendMessage(Message message, SendMessageCallback callback) {
    auto [addressPtr, addressLen] = message.address.release();
    auto [payloadPtr, payloadLen] = message.payload.release();
    _eventLoopThread->_eventLoop.executeNow(
        [this, addressPtr, addressLen, payloadPtr, payloadLen, callback = std::move(callback)] {
            auto payload = std::make_shared<std::vector<char>>(8);
            payload->insert(payload->end(), payloadPtr, payloadPtr + payloadLen);
            *(uint64_t*)payload->data() = payloadLen;

            MessageConnectionTCP* conn = nullptr;
            std::string address        = std::string(addressPtr, addressLen);

            if (this->_identityToConnection.contains(address)) {
                conn = this->_identityToConnection[address].get();
            } else {
                auto it = std::ranges::find(
                    _unestablishedConnection, address, &MessageConnectionTCP::_remoteIOSocketIdentity);
                if (it != _unestablishedConnection.end()) {
                    conn = it->get();
                } else {
                    onConnectionCreated(0, {}, {}, false, address);
                    conn = _unestablishedConnection.back().get();
                }
            }
            conn->sendMessage(std::move(payload), std::move(callback));
        });
}

void IOSocket::recvMessage(RecvMessageCallback callback) {
    _eventLoopThread->_eventLoop.executeNow([this, callback = std::move(callback)] {
        TcpReadOperation readOp {std::make_shared<std::vector<char>>(), 0, std::move(callback)};
        this->_pendingReadOperations->push(std::move(readOp));
        if (_pendingReadOperations->size() == 1) {
            for (const auto& [fd, conn]: _identityToConnection) {
                if (conn->recvMessage())
                    return;
            }
        }
    });
}

void IOSocket::onConnectionCreated(
    int fd,
    sockaddr localAddr,
    sockaddr remoteAddr,
    bool responsibleForRetry,
    std::optional<std::string> remoteIOSocketIdentity) {
    _unestablishedConnection.push_back(
        std::make_unique<MessageConnectionTCP>(
            _eventLoopThread,
            fd,
            std::move(localAddr),
            std::move(remoteAddr),
            this->identity(),
            responsibleForRetry,
            _pendingReadOperations,
            std::move(remoteIOSocketIdentity)));
    _unestablishedConnection.back()->onCreated();
}

IOSocket::~IOSocket() {
    while (_pendingReadOperations->size()) {
        auto readOp = std::move(_pendingReadOperations->front());
        _pendingReadOperations->pop();
        readOp._callbackAfterCompleteRead({});
    }
}

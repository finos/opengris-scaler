
#include "scaler/io/ymq/message_connection_tcp.h"

#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <optional>

#include "scaler/io/ymq/event_loop_thread.h"
#include "scaler/io/ymq/event_manager.h"
#include "scaler/io/ymq/io_socket.h"

static bool isCompleteMessage(const std::vector<char>& vec) {
    if (vec.size() < 8)
        return false;
    uint64_t size = *(uint64_t*)vec.data();
    return vec.size() == size + 8;
}

MessageConnectionTCP::MessageConnectionTCP(
    std::shared_ptr<EventLoopThread> eventLoopThread,
    int connFd,
    sockaddr localAddr,
    sockaddr remoteAddr,
    std::string localIOSocketIdentity,
    bool responsibleForRetry,
    std::shared_ptr<std::queue<TcpReadOperation>> pendingReadOperations,
    std::optional<std::string> remoteIOSocketIdentity)
    : _eventLoopThread(eventLoopThread)
    , _eventManager(std::make_unique<EventManager>())
    , _connFd(std::move(connFd))
    , _localAddr(std::move(localAddr))
    , _remoteAddr(std::move(remoteAddr))
    , _localIOSocketIdentity(std::move(localIOSocketIdentity))
    , _remoteIOSocketIdentity(std::move(remoteIOSocketIdentity))
    , _sendLocalIdentity(false)
    , _responsibleForRetry(responsibleForRetry)
    , _pendingReadOperations(pendingReadOperations) {
    _eventManager->onRead  = [this] { this->onRead(); };
    _eventManager->onWrite = [this] { this->onWrite(); };
    _eventManager->onClose = [this] { this->onClose(); };
    _eventManager->onError = [this] { this->onError(); };
}

void MessageConnectionTCP::onCreated() {
    if (_connFd != 0) {
        this->_eventLoopThread->_eventLoop.addFdToLoop(
            _connFd, EPOLLIN | EPOLLOUT | EPOLLET, this->_eventManager.get());
    }
}

void MessageConnectionTCP::onRead() {
    // TODO:
    // - do not assume the identity to be less than 128bytes
    // - do not make remoteIOSocketIdentity a failing point
    if (!_remoteIOSocketIdentity) {
        // Other sizes are possible, but the size needs to be >= 8, in order for idBuf
        // to be aligned with 8 bytes boundary because of strict aliasing
        uint64_t header {};
        int n = read(_connFd, &header, 8);
        char idBuf[128] {};
        n           = read(_connFd, idBuf, header);
        char* first = idBuf;
        std::string remoteID(first, first + header);
        _remoteIOSocketIdentity.emplace(std::move(remoteID));
        auto& sock = this->_eventLoopThread->_identityToIOSocket[_localIOSocketIdentity];
        sock->onConnectionIdentityReceived(this);
    }

    while (true) {
        const size_t headerSize = 8;
        size_t leftOver         = 0;
        size_t first            = 0;

        // Good case
        if (_receivedMessages.empty() || isCompleteMessage(_receivedMessages.back())) {
            _receivedMessages.push({});
            _receivedMessages.back().resize(1024);
            leftOver = headerSize;
            first    = 0;
        } else {
            // Bad case, we currently have an incomplete message
            auto& message = _receivedMessages.back();
            if (message.size() < headerSize) {
                leftOver = headerSize - message.size();
                first    = message.size();
                message.resize(1024);
            } else {
                size_t payloadSize = *(uint64_t*)message.data();

                if (message.size() == 1024) {
                    message.resize(std::min((payloadSize), 8lu));
                }

                leftOver = payloadSize - (message.size() - headerSize);

                first = message.size();
                message.resize(payloadSize + headerSize);
            }
        }

        auto& message = _receivedMessages.back();

        while (leftOver) {
            assert(first + leftOver <= message.size());
            int res = read(_connFd, message.data() + first, leftOver);

            if (res == 0) {
                onClose();
                return;
            }

            if (res == -1 && errno == EAGAIN) {
                perror("read");
                message.resize(first);
                // Reviewer: To jump out of double loop, reconsider when you say no. - gxu
                goto ReadExhuasted;
            }

            leftOver -= res;
            first += res;
            if (first == headerSize) {
                // reading the payload
                leftOver = *(uint64_t*)message.data();
                message.resize(leftOver + headerSize);
            }
        }
        assert(isCompleteMessage(_receivedMessages.back()));
    }

ReadExhuasted:
    printf("READ EXHAUSTED\n");
    while (_pendingReadOperations->size() && _receivedMessages.size()) {
        if (isCompleteMessage(_receivedMessages.front())) {
            *_pendingReadOperations->front()._buf = std::move(_receivedMessages.front());
            _receivedMessages.pop();

            Bytes address(_remoteIOSocketIdentity->data(), _remoteIOSocketIdentity->size());
            Bytes payload(
                _pendingReadOperations->front()._buf->data() + 8, _pendingReadOperations->front()._buf->size() - 8);

            _pendingReadOperations->front()._callbackAfterCompleteRead(Message(std::move(address), std::move(payload)));

            _pendingReadOperations->pop();
        } else {
            assert(_pendingReadOperations->size());
            break;
        }
    }
}

void MessageConnectionTCP::onWrite() {
    if (_connFd == 0) {
        return;
    }

    // TODO: do not assume the identity to be less than 128bytes
    if (!_sendLocalIdentity) {
        char idBuf[128] {};
        *(uint64_t*)idBuf  = _localIOSocketIdentity.size();
        auto identityBegin = idBuf + sizeof(uint64_t);
        auto [_, last]     = std::ranges::copy(_localIOSocketIdentity, identityBegin);
        write(_connFd, idBuf, std::distance(idBuf, last));
        _sendLocalIdentity = true;
    }

    while (!_writeOperations.empty()) {
        auto& writeOp       = _writeOperations.front();
        const size_t bufLen = writeOp._buf->size();
        while (writeOp._cursor != bufLen) {
            const size_t leftOver = writeOp._buf->size() - writeOp._cursor;
            const char* begin     = writeOp._buf->data() + writeOp._cursor;
            auto bytes            = write(_connFd, begin, leftOver);

            if (bytes == -1) {
                if (errno == EAGAIN)
                    break;
                else {
                    perror("write");
                    writeOp._callbackAfterCompleteWrite(errno);
                }
            }

            writeOp._cursor += bytes;
        }

        if (writeOp._cursor == bufLen) {
            std::string str(
                _writeOperations.front()._buf->data() + 8,
                _writeOperations.front()._buf->data() + _writeOperations.front()._buf->size());

            writeOp._callbackAfterCompleteWrite(0);
            _writeOperations.pop();
        } else {
            break;
        }
    }
}

void MessageConnectionTCP::onClose() {
    _eventLoopThread->_eventLoop.removeFdFromLoop(_connFd);
    close(_connFd);
    auto& sock = _eventLoopThread->_identityToIOSocket.at(_localIOSocketIdentity);
    sock->onConnectionDisconnected(this);
    _connFd = 0;
};

// TODO: Maybe change this to message_t
void MessageConnectionTCP::sendMessage(std::shared_ptr<std::vector<char>> msg, std::function<void(int)> callback) {
    // detect if the write operations queue is empty, if it is, simply write to exhaustion
    // if it is not, queue write operations to the end of the queue
    TcpWriteOperation writeOp;
    writeOp._buf                        = msg;
    writeOp._cursor                     = 0;
    writeOp._callbackAfterCompleteWrite = std::move(callback);

    if (_connFd == 0) {
        _writeOperations.push(std::move(writeOp));
        return;
    }

    if (_writeOperations.size()) {
        _writeOperations.push(std::move(writeOp));
        return;
    }

    const size_t bufLen = writeOp._buf->size();
    while (writeOp._cursor != bufLen) {
        const size_t leftOver = writeOp._buf->size() - writeOp._cursor;
        const char* begin     = writeOp._buf->data() + writeOp._cursor;
        auto bytes            = write(_connFd, begin, leftOver);

        if (bytes == -1) {
            if (errno == EAGAIN) {
                _writeOperations.push(std::move(writeOp));
            } else {
                perror("write");
                writeOp._callbackAfterCompleteWrite(errno);
            }
            break;
        }

        writeOp._cursor += bytes;
    }

    if (writeOp._cursor == bufLen) {
        writeOp._callbackAfterCompleteWrite(0);
    }
}

bool MessageConnectionTCP::recvMessage() {
    if (_receivedMessages.empty() || _pendingReadOperations->empty() || !isCompleteMessage(_receivedMessages.front())) {
        return false;
    }

    while (_pendingReadOperations->size() && _receivedMessages.size()) {
        if (isCompleteMessage(_receivedMessages.front())) {
            *_pendingReadOperations->front()._buf = std::move(_receivedMessages.front());
            _receivedMessages.pop();

            Bytes address(_remoteIOSocketIdentity->data(), _remoteIOSocketIdentity->size());
            Bytes payload(
                _pendingReadOperations->front()._buf->data() + 8, _pendingReadOperations->front()._buf->size() - 8);

            _pendingReadOperations->front()._callbackAfterCompleteRead(Message(std::move(address), std::move(payload)));

            _pendingReadOperations->pop();
        } else {
            assert(_pendingReadOperations->size());
            break;
        }
    }
    return true;
}

MessageConnectionTCP::~MessageConnectionTCP() {
    if (_connFd != 0) {
        _eventLoopThread->_eventLoop.removeFdFromLoop(_connFd);
        shutdown(_connFd, SHUT_RDWR);
        close(_connFd);
        _connFd = 0;
    }

    while (_writeOperations.size()) {
        auto writeOp = std::move(_writeOperations.front());
        _writeOperations.pop();
        writeOp._callbackAfterCompleteWrite(-1);
    }

    // TODO: What to do with this?
    // std::queue<std::vector<char>> _receivedMessages;
}

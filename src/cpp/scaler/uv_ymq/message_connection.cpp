#include "scaler/uv_ymq/message_connection.h"

#include <algorithm>
#include <array>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <span>
#include <utility>
#include <variant>

#include "scaler/wrapper/uv/pipe.h"
#include "scaler/wrapper/uv/tcp.h"

namespace scaler {
namespace uv_ymq {

MessageConnection::MessageConnection(
    scaler::wrapper::uv::Loop& loop,
    Identity localIdentity,
    std::optional<Identity> remoteIdentity,
    RemoteIdentityCallback onRemoteIdentityCallback,
    RemoteDisconnectCallback onRemoteDisconnectCallback,
    RecvMessageCallback onRecvMessageCallback) noexcept
    : _loop(loop)
    , _localIdentity(std::move(localIdentity))
    , _remoteIdentity(std::move(remoteIdentity))
    , _onRemoteIdentityCallback(std::move(onRemoteIdentityCallback))
    , _onRemoteDisconnectCallback(std::move(onRemoteDisconnectCallback))
    , _onRecvMessageCallback(std::move(onRecvMessageCallback))
{
    sendLocalIdentity();
}

MessageConnection::~MessageConnection() noexcept
{
    if (connected()) {
        disconnect();
    }

    // Fail all pending send operations
    while (!_sendPending.empty()) {
        auto& callback = _sendPending.front()._onMessageSent;
        callback(std::unexpected(scaler::ymq::Error(scaler::ymq::Error::ErrorCode::IOSocketStopRequested)));
        _sendPending.pop();
    }
}

MessageConnection::State MessageConnection::state() const noexcept
{
    return _state;
}

bool MessageConnection::connected() const noexcept
{
    return _state == State::Connected || _state == State::Established;
}

bool MessageConnection::established() const noexcept
{
    return _state == State::Established;
}

void MessageConnection::connect(Client client) noexcept
{
    assert(!connected());

    _client = std::move(client);
    _state  = State::Connected;

    readStart();
    processSendQueue();
}

void MessageConnection::disconnect() noexcept
{
    assert(connected());

    readStop();
    _recvCurrent = RecvOperation {};

    _client = std::nullopt;
    _state  = State::Disconnected;

    sendLocalIdentity();  // enqueue the first identity message in case we reconnect.
}

const Identity& MessageConnection::localIdentity() const noexcept
{
    return _localIdentity;
}

const std::optional<Identity>& MessageConnection::remoteIdentity() const noexcept
{
    return _remoteIdentity;
}

void MessageConnection::sendMessage(scaler::ymq::Message message, SendMessageCallback onMessageSent) noexcept
{
    SendOperation operation;
    operation._payload       = std::move(message.payload);
    operation._onMessageSent = std::move(onMessageSent);
    operation._payloadSize   = operation._payload.size();

    _sendPending.push(std::move(operation));

    if (connected()) {
        processSendQueue();
    }
}

void MessageConnection::onWriteDone(
    SendMessageCallback callback, std::expected<void, scaler::wrapper::uv::Error> result) noexcept
{
    if (!result.has_value()) {
        switch (result.error().code()) {
            case UV_ECONNRESET:
            case UV_EPIPE:
                // Connection closed/failed WHILE libuv issued the write to the OS.
                // No need to handle this disconnect event, as this will be handled by onRead().
                return;
            case UV_ECANCELED:
                // Connection closed/failed BEFORE libuv issued the write to the OS.
                // FIXME: as we are certain these bytes haven't been issued on the wire, we could requeue these messages
                // in case the connection is later re-established. But we can't be sure the MessageConnection object is
                // still live, as this callback might be called after the connection object got destroyed.
                return;
            default:
                // Unexpected error
                UV_EXIT_ON_ERROR(result);
                break;
        };
    }

    callback({});
}

void MessageConnection::onRead(std::expected<std::span<const uint8_t>, scaler::wrapper::uv::Error> result) noexcept
{
    assert(connected());

    if (!result.has_value()) {
        switch (result.error().code()) {
            case UV_EOF:
                // Remote explicitly closed the connection (graceful disconnect)
                onRemoteDisconnect(DisconnectReason::Disconnected);
                return;
            case UV_ECONNRESET:
            case UV_ECONNABORTED:
            case UV_ETIMEDOUT:
            case UV_EPIPE:
            case UV_ENETDOWN:
                // Connection aborted unexpectedly
                onRemoteDisconnect(DisconnectReason::Aborted);
                return;
            default:
                // Unexpected error
                UV_EXIT_ON_ERROR(result);
                return;
        }
    }

    const std::span<const uint8_t> data = result.value();
    size_t offset                       = 0;

    while (offset < data.size()) {
        // Read header
        if (_recvCurrent._cursor < HEADER_SIZE) {
            offset += readHeader(data.subspan(offset));

            // Allocate payload buffer
            if (_recvCurrent._cursor == HEADER_SIZE) {
                bool success = allocatePayload();

                if (!success) {
                    _logger.log(
                        scaler::ymq::Logger::LoggingLevel::error,
                        "Failed to allocate ",
                        _recvCurrent._header,
                        " bytes.");
                    onRemoteDisconnect(DisconnectReason::Aborted);
                    return;
                }
            }
        }

        // Read payload
        if (_recvCurrent._cursor >= HEADER_SIZE) {
            offset += readPayload(data.subspan(offset));
        }

        // Read message if completed
        if (_recvCurrent._cursor == HEADER_SIZE + _recvCurrent._header) {
            scaler::ymq::Message message;

            if (_remoteIdentity.has_value()) {
                message.address = scaler::ymq::Bytes(*_remoteIdentity);
            }
            message.payload = std::move(_recvCurrent._payload);

            onMessage(std::move(message));

            _recvCurrent = RecvOperation {};
        }
    }
}

void MessageConnection::onMessage(scaler::ymq::Message message) noexcept
{
    assert(connected());

    // First message received is the remote identity
    if (!established()) {
        onRemoteIdentity(std::move(message));
        return;
    }

    _onRecvMessageCallback(std::move(message));
}

void MessageConnection::onRemoteIdentity(scaler::ymq::Message message) noexcept
{
    assert(connected());
    assert(!established());

    Identity receivedIdentity {reinterpret_cast<const char*>(message.payload.data()), message.payload.size()};

    if (_remoteIdentity.has_value() && *_remoteIdentity != receivedIdentity) {
        _logger.log(
            scaler::ymq::Logger::LoggingLevel::error,
            "Received identity (",
            receivedIdentity,
            ") does not match previously known identity (",
            *_remoteIdentity,
            ")");
        onRemoteDisconnect(DisconnectReason::Aborted);
        return;
    }

    _remoteIdentity = std::move(receivedIdentity);
    _state          = State::Established;
    _onRemoteIdentityCallback({*_remoteIdentity});
}

void MessageConnection::onRemoteDisconnect(MessageConnection::DisconnectReason reason) noexcept
{
    assert(connected());

    disconnect();

    _onRemoteDisconnectCallback(reason);
}

void MessageConnection::sendLocalIdentity() noexcept
{
    assert(_sendPending.empty() && "Identity should be the first message");

    scaler::ymq::Message message;
    message.address = scaler::ymq::Bytes();
    message.payload = scaler::ymq::Bytes(_localIdentity.data(), _localIdentity.size());

    SendMessageCallback callback = [](std::expected<void, scaler::ymq::Error> result) {};

    sendMessage(std::move(message), std::move(callback));
}

void MessageConnection::processSendQueue() noexcept
{
    assert(connected());

    while (!_sendPending.empty()) {
        // Move operation out of queue and into a unique_ptr to ensure it stays alive during the async write
        auto operation = std::make_unique<SendOperation>(std::move(_sendPending.front()));
        _sendPending.pop();

        std::array<std::span<const uint8_t>, 2> buffers = {{
            std::span<const uint8_t> {
                reinterpret_cast<const uint8_t*>(&operation->_payloadSize), HEADER_SIZE},      // header
            std::span<const uint8_t> {operation->_payload.data(), operation->_payload.size()}  // payload
        }};

        // Capture the operation to keep it alive until callback completes
        auto callback = [operation =
                             std::move(operation)](std::expected<void, scaler::wrapper::uv::Error> result) mutable {
            MessageConnection::onWriteDone(std::move(operation->_onMessageSent), std::move(result));
        };

        UV_EXIT_ON_ERROR(write(buffers, std::move(callback)));
    }
}

std::expected<scaler::wrapper::uv::WriteRequest, scaler::wrapper::uv::Error> MessageConnection::write(
    std::span<const std::span<const uint8_t>> buffers, scaler::wrapper::uv::WriteCallback callback) noexcept
{
    assert(connected());

    if (auto* tcpSocket = std::get_if<scaler::wrapper::uv::TCPSocket>(&_client.value())) {
        return tcpSocket->write(buffers, std::move(callback));
    } else if (auto* pipe = std::get_if<scaler::wrapper::uv::Pipe>(&_client.value())) {
        return pipe->write(buffers, std::move(callback));
    } else {
        std::unreachable();
    }
}

void MessageConnection::readStart() noexcept
{
    assert(connected());

    if (auto* tcpSocket = std::get_if<scaler::wrapper::uv::TCPSocket>(&_client.value())) {
        UV_EXIT_ON_ERROR(tcpSocket->readStart(std::bind_front(&MessageConnection::onRead, this)));
    } else if (auto* pipe = std::get_if<scaler::wrapper::uv::Pipe>(&_client.value())) {
        UV_EXIT_ON_ERROR(pipe->readStart(std::bind_front(&MessageConnection::onRead, this)));
    } else {
        std::unreachable();
    }
}

void MessageConnection::readStop() noexcept
{
    assert(connected());

    if (auto* tcpSocket = std::get_if<scaler::wrapper::uv::TCPSocket>(&_client.value())) {
        tcpSocket->readStop();
    } else if (auto* pipe = std::get_if<scaler::wrapper::uv::Pipe>(&_client.value())) {
        pipe->readStop();
    } else {
        std::unreachable();
    }
}

size_t MessageConnection::readHeader(std::span<const uint8_t> data) noexcept
{
    uint8_t* readDest = reinterpret_cast<uint8_t*>(&_recvCurrent._header) + _recvCurrent._cursor;
    size_t readCount  = std::min(HEADER_SIZE - _recvCurrent._cursor, data.size());

    std::memcpy(readDest, data.data(), readCount);

    _recvCurrent._cursor += readCount;

    return readCount;
}

size_t MessageConnection::readPayload(std::span<const uint8_t> data) noexcept
{
    size_t payloadSize   = _recvCurrent._header;
    size_t payloadOffset = _recvCurrent._cursor - HEADER_SIZE;

    uint8_t* readDest = _recvCurrent._payload.data() + payloadOffset;
    size_t readCount  = std::min(payloadSize - payloadOffset, data.size());

    std::memcpy(readDest, data.data(), readCount);

    _recvCurrent._cursor += readCount;

    return readCount;
}

bool MessageConnection::allocatePayload() noexcept
{
    if (_recvCurrent._header > 0) {
        try {
            _recvCurrent._payload = scaler::ymq::Bytes::alloc(_recvCurrent._header);
        } catch (const std::bad_alloc& e) {
            return false;
        }
    }

    return true;
}

}  // namespace uv_ymq
}  // namespace scaler

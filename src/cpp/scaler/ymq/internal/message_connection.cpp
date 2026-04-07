#include "scaler/ymq/internal/message_connection.h"

#include <cassert>
#include <cstring>
#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include "scaler/ymq/configuration.h"

namespace scaler {
namespace ymq {
namespace internal {

MessageConnection::MessageConnection(
    Identity localIdentity,
    std::optional<Identity> remoteIdentity,
    RemoteIdentityCallback onRemoteIdentityCallback,
    RemoteDisconnectCallback onRemoteDisconnectCallback,
    RecvMessageCallback onRecvMessageCallback) noexcept
    : _localIdentity(std::move(localIdentity))
    , _remoteIdentity(std::move(remoteIdentity))
    , _onRemoteIdentityCallback(std::move(onRemoteIdentityCallback))
    , _onRemoteDisconnectCallback(std::move(onRemoteDisconnectCallback))
    , _onRecvMessageCallback(std::move(onRecvMessageCallback))
{
    initialize();
}

MessageConnection::~MessageConnection() noexcept
{
    if (connected()) {
        shutdownClient();
    }

    // Fail all pending send operations
    while (!_sendPending.empty()) {
        auto& callback = _sendPending.front()._onSendDone;
        callback(std::unexpected(Error {Error::ErrorCode::SocketStopRequested}));
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

    UV_EXIT_ON_ERROR(_client->setNoDelay(true));
    UV_EXIT_ON_ERROR(_client->readStart(std::bind_front(&MessageConnection::onRead, this)));
    processSendQueue();
}

void MessageConnection::disconnect() noexcept
{
    assert(connected());

    shutdownClient();

    initialize();
}

void MessageConnection::abort() noexcept
{
    assert(connected());

    _client->readStop();
    UV_EXIT_ON_ERROR(_client->closeReset());

    initialize();
}

const Identity& MessageConnection::localIdentity() const noexcept
{
    return _localIdentity;
}

const std::optional<Identity>& MessageConnection::remoteIdentity() const noexcept
{
    return _remoteIdentity;
}

void MessageConnection::sendMessage(Bytes messagePayload, SendMessageCallback onMessageSent) noexcept
{
    // Heap allocate the header buffer until the send completes.
    std::unique_ptr<Header> header = std::make_unique<Header>(messagePayload.size());

    const std::vector<std::span<const uint8_t>> buffers {
        std::span<const uint8_t> {reinterpret_cast<const uint8_t*>(header.get()), sizeof(Header)},  // header
        std::span<const uint8_t> {messagePayload.data(), messagePayload.size()}                     // payload
    };

    send(
        std::move(buffers),
        [header         = std::move(header),
         messagePayload = std::move(messagePayload),
         onMessageSent  = std::move(onMessageSent)](std::expected<void, Error> result) mutable {
            onMessageSent(std::move(result));
        });
}

void MessageConnection::shutdownClient() noexcept
{
    assert(connected());

    _client->readStop();

    // Call shutdown() on the client socket *before* closing it. This forces a FIN segment.
    // We transfer ownership of the Client instance to the shutdown callback. As the Client destructor implicitly calls
    // close(), the instance must remain alive until shutdown() completes, or else it might trigger a RST segment.
    // By moving its ownership to the shutdown()'s callback, close() is guaranteed to be called only after the callback
    // completes.

    auto client       = std::make_unique<Client>(std::move(_client.value()));
    Client* clientPtr = client.get();

    auto shutdownCallback = [client =
                                 std::move(client)](std::expected<void, scaler::wrapper::uv::Error> result) noexcept {
        UV_EXIT_ON_ERROR(result);
    };

    UV_EXIT_ON_ERROR(clientPtr->shutdown(std::move(shutdownCallback)));
}

void MessageConnection::initialize() noexcept
{
    _client      = std::nullopt;
    _state       = State::Disconnected;
    _recvCurrent = RecvOperation {};

    sendHandshake();
    recvMagicNumber();
}

void MessageConnection::send(std::vector<std::span<const uint8_t>> buffers, SendCallback callback) noexcept
{
    SendOperation operation;
    operation._buffers    = std::move(buffers);
    operation._onSendDone = std::move(callback);

    _sendPending.push(std::move(operation));

    if (connected()) {
        processSendQueue();
    }
}

void MessageConnection::recv(size_t size, RecvCallback callback) noexcept
{
    assert(_recvCurrent._cursor == _recvCurrent._buffer.size() && "previous recv() call not yet completed");

    _recvCurrent._cursor = 0;

    try {
        _recvCurrent._buffer = Bytes::alloc(size);
    } catch (const std::bad_alloc& e) {
        _logger.log(Logger::LoggingLevel::error, "Failed to allocate ", size, " bytes.");
        onRemoteDisconnect(DisconnectReason::Aborted);
        return;
    }

    if (size == 0) {
        // Empty read, complete immediately with an empty buffer.
        callback(std::move(_recvCurrent._buffer));
        return;
    }

    _recvCurrent._onRecvDone = std::move(callback);
}

void MessageConnection::sendHandshake() noexcept
{
    assert(_sendPending.empty() && "handshake should be sent first");

    // Magic string
    const std::vector<std::span<const uint8_t>> magicStringBuffer {std::span<const uint8_t> {magicString}};
    send(std::move(magicStringBuffer), []([[maybe_unused]] std::expected<void, Error> result) {});

    // Identity
    const Bytes identityBytes {_localIdentity.data(), _localIdentity.size()};
    sendMessage(std::move(identityBytes), []([[maybe_unused]] std::expected<void, Error> result) {});
}

void MessageConnection::recvMagicNumber() noexcept
{
    recv(magicString.size(), [this](Bytes payload) {
        assert(connected());
        assert(payload.size() == magicString.size());

        const bool magicIsValid = std::memcmp(payload.data(), magicString.data(), magicString.size()) == 0;

        if (!magicIsValid) {
            _logger.log(Logger::LoggingLevel::error, "Invalid YMQ magic string received");
            onRemoteDisconnect(DisconnectReason::Aborted);
            return;
        }

        recvMessage();  // next, expect the remote identity message.
    });
}

void MessageConnection::recvMessage() noexcept
{
    recv(sizeof(Header), [this](Bytes headerPayload) {
        assert(connected());

        Header header;
        std::memcpy(&header, headerPayload.data(), sizeof(Header));

        recv(header, [this](Bytes messagePayload) {
            assert(connected());

            if (!established()) {
                // First message received is the remote identity
                onRemoteIdentity(std::move(messagePayload));
            } else {
                _onRecvMessageCallback(std::move(messagePayload));
            }

            recvMessage();  // next message
        });
    });
}

void MessageConnection::onWriteDone(
    SendMessageCallback callback, std::expected<void, scaler::wrapper::uv::Error> result) noexcept
{
    if (!result.has_value()) {
        switch (result.error().code()) {
            case UV_ECONNRESET:
            case UV_ECONNABORTED:
            case UV_ETIMEDOUT:
            case UV_EPIPE:
            case UV_ENETDOWN:
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

    while (offset < data.size() && connected()) {
        // Read into the current receive buffer
        assert(_recvCurrent._cursor < _recvCurrent._buffer.size() && "no receive operation in progress");

        const size_t readCount = std::min(_recvCurrent._buffer.size() - _recvCurrent._cursor, data.size() - offset);
        uint8_t* readDest      = _recvCurrent._buffer.data() + _recvCurrent._cursor;

        std::memcpy(readDest, data.subspan(offset).data(), readCount);

        _recvCurrent._cursor += readCount;
        offset += readCount;

        // If the receive buffer is full, invoke the callback
        if (_recvCurrent._cursor == _recvCurrent._buffer.size()) {
            _recvCurrent._onRecvDone(_recvCurrent._buffer);
        }
    }
}

void MessageConnection::onRemoteIdentity(Bytes payload) noexcept
{
    assert(connected());
    assert(!established());

    Identity receivedIdentity = payload.as_string().value();

    if (_remoteIdentity.has_value() && *_remoteIdentity != receivedIdentity) {
        _logger.log(
            Logger::LoggingLevel::error,
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
    _onRemoteIdentityCallback(*_remoteIdentity);
}

void MessageConnection::onRemoteDisconnect(MessageConnection::DisconnectReason reason) noexcept
{
    assert(connected());

    _client->readStop();
    initialize();

    _onRemoteDisconnectCallback(reason);
}

void MessageConnection::processSendQueue() noexcept
{
    assert(connected());

    while (!_sendPending.empty()) {
        SendOperation operation = std::move(_sendPending.front());
        _sendPending.pop();

        processSendOperation(std::move(operation));
    }
}

void MessageConnection::processSendOperation(SendOperation operation) noexcept
{
    // Calculate total size of all buffers
    size_t totalSize = 0;
    for (const auto& buffer: operation._buffers) {
        totalSize += buffer.size();
    }

    // Make the callback own the operation's callback
    auto callback = [onSendDone = std::move(operation._onSendDone)](
                        std::expected<void, scaler::wrapper::uv::Error> result) mutable {
        onWriteDone(std::move(onSendDone), std::move(result));
    };

    if (totalSize <= maxWriteBufferSize) {
        // Small message: all buffers in one syscall
        UV_EXIT_ON_ERROR(_client->write(
            std::span<const std::span<const uint8_t>> {operation._buffers.data(), operation._buffers.size()},
            std::move(callback)));
    } else {
        // Large message: chunk the buffers in write() calls of up to maxWriteBufferSize.
        //
        // Not doing this makes some OSes fail (macOS, Windows) with EINVAL as these don't support large writes.
        size_t offset = 0;
        for (const auto& buffer: operation._buffers) {
            for (size_t bufferOffset = 0; bufferOffset < buffer.size(); bufferOffset += maxWriteBufferSize) {
                const size_t chunkSize = std::min(buffer.size() - bufferOffset, maxWriteBufferSize);
                const std::span<const uint8_t> chunk {buffer.data() + bufferOffset, chunkSize};

                const bool isLastChunk = (offset + bufferOffset + chunkSize >= totalSize);

                if (!isLastChunk) {
                    UV_EXIT_ON_ERROR(_client->write(std::span(&chunk, 1), [](auto) {}));
                } else {
                    // Attach the callback to the last write() call.
                    UV_EXIT_ON_ERROR(_client->write(std::span(&chunk, 1), std::move(callback)));
                }
            }
            offset += buffer.size();
        }
    }
}

}  // namespace internal
}  // namespace ymq
}  // namespace scaler

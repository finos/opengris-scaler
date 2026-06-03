#include "scaler/wrapper/openssl/secure_socket.h"

#include <algorithm>
#include <array>
#include <cassert>
#include <climits>
#include <functional>
#include <utility>
#include <vector>

namespace scaler {
namespace wrapper {
namespace openssl {

std::expected<SecureSocket, uv::Error> SecureSocket::init(SSLContext context, uv::TCPSocket transport) noexcept
{
    // Create SSL session
    SSLPtr<SSL> ssl {SSL_new(context.native())};
    if (ssl == nullptr) {
        return std::unexpected {uv::Error {UV_ENOMEM}};
    }

    // Create and configure BIO buffers
    SSLPtr<BIO> readBIO {BIO_new(BIO_s_mem())};
    SSLPtr<BIO> writeBIO {BIO_new(BIO_s_mem())};
    if (readBIO == nullptr || writeBIO == nullptr) {
        return std::unexpected {uv::Error {UV_ENOMEM}};
    }

    // Associate BIOs with the SSL session.
    BIO_up_ref(readBIO.get());  // BIO_up_ref() is required as SSL_set_bio() takes ownership of these buffers.
    BIO_up_ref(writeBIO.get());
    SSL_set_bio(ssl.get(), readBIO.get(), writeBIO.get());

    return SecureSocket {
        std::move(context), std::move(transport), std::move(ssl), std::move(readBIO), std::move(writeBIO)};
}

SecureSocket::SecureSocket(
    SSLContext context, uv::TCPSocket transport, SSLPtr<SSL> ssl, SSLPtr<BIO> readBIO, SSLPtr<BIO> writeBIO) noexcept
    : _context(std::move(context))
    , _transport(std::move(transport))
    , _ssl(std::move(ssl))
    , _readBIO(std::move(readBIO))
    , _writeBIO(std::move(writeBIO))
{
}

std::expected<uv::ConnectRequest, uv::Error> SecureSocket::connect(
    const uv::SocketAddress& address, uv::ConnectCallback callback) noexcept
{
    if (_state != State::Uninitialized) {
        return std::unexpected {uv::Error {UV_EINVAL}};
    }

    _state               = State::Connecting;
    _onHandshakeCallback = std::move(callback);

    std::expected<uv::ConnectRequest, uv::Error> result =
        _transport.connect(address, std::bind_front(&SecureSocket::onTransportConnected, this));
    if (!result.has_value()) {
        failWithError(result.error());
        return result;
    }

    return result;
}

std::expected<void, uv::Error> SecureSocket::accept(uv::ConnectCallback callback) noexcept
{
    if (_state != State::Uninitialized) {
        return std::unexpected {uv::Error {UV_EINVAL}};
    }

    _onHandshakeCallback = std::move(callback);

    return startHandshake(HandshakeMode::Accept);
}

std::expected<void, uv::Error> SecureSocket::readStart(uv::ReadCallback callback) noexcept
{
    if (_state == State::Closing || _state == State::Closed) {
        return std::unexpected {uv::Error {UV_EPIPE}};
    }

    // We do not propagate readStart() to the transport layer, as we already enabled it during the SSL handshake.
    // We instead "mask" and queue the reads until the application layer registers a callback.

    _onReadCallback = std::move(callback);

    return flushToApplication();
}

void SecureSocket::readStop() noexcept
{
    _onReadCallback.reset();
}

std::expected<void, uv::Error> SecureSocket::write(
    std::span<const std::span<const uint8_t>> buffers, uv::WriteCallback callback) noexcept
{
    if (_state == State::Closing || _state == State::Closed) {
        return std::unexpected {uv::Error {UV_EPIPE}};
    }

    if (buffers.empty()) {
        callback({});
        return {};
    }

    // Queue each buffer as a single PendingWrite object.
    for (const auto& buffer: buffers) {
        // Only attach the callback to the last buffer; earlier get a noop.
        auto bufferCallback = &buffer == &buffers.back() ? std::move(callback) : [](std::expected<void, uv::Error>) {};

        _pendingWrites.push_back(PendingWrite {buffer, std::move(bufferCallback)});
    }

    return processPendingWrites();
}

std::expected<void, uv::Error> SecureSocket::write(std::span<const uint8_t> buffer, uv::WriteCallback callback) noexcept
{
    const std::span<const std::span<const uint8_t>> buffers {&buffer, 1};
    return write(buffers, std::move(callback));
}

std::expected<void, uv::Error> SecureSocket::shutdown(uv::ShutdownCallback callback) noexcept
{
    if (_state == State::Closing || _state == State::Closed) {
        return std::unexpected {uv::Error {UV_EPIPE}};
    }

    _state              = State::Closing;
    _onShutdownCallback = std::move(callback);

    // Drain pending writes first. We will initiate the SSL shutdown once the write queue is empty.
    return processPendingWrites();
}

std::expected<void, uv::Error> SecureSocket::closeReset() noexcept
{
    if (_state == State::Closed) {
        return std::unexpected {uv::Error {UV_EINVAL}};
    }

    // Follow libuv behavior on close-reset, by stopping reads and calling remaining callbacks with UV_ECANCELED.
    readStop();
    failWithError(uv::Error {UV_ECANCELED});

    return _transport.closeReset();
}

std::expected<uv::SocketAddress, uv::Error> SecureSocket::getSockName() const noexcept
{
    return _transport.getSockName();
}

std::expected<uv::SocketAddress, uv::Error> SecureSocket::getPeerName() const noexcept
{
    return _transport.getPeerName();
}

std::expected<void, uv::Error> SecureSocket::nodelay(bool enable) noexcept
{
    return _transport.nodelay(enable);
}

SecureSocket::State SecureSocket::state() const noexcept
{
    return _state;
}

bool SecureSocket::established() const noexcept
{
    return _state == State::Established;
}

uv::TCPSocket& SecureSocket::transport() noexcept
{
    return _transport;
}

std::expected<void, uv::Error> SecureSocket::startHandshake(HandshakeMode mode) noexcept
{
    assert(_state == State::Uninitialized || _state == State::Connecting);
    assert(_onHandshakeCallback.has_value());

    _state = State::Handshaking;

    if (mode == HandshakeMode::Connect) {
        SSL_set_connect_state(_ssl.get());
    } else {
        SSL_set_accept_state(_ssl.get());
    }

    std::expected<void, uv::Error> readResult =
        _transport.readStart(std::bind_front(&SecureSocket::onTransportRead, this));
    if (!readResult.has_value()) {
        failWithError(readResult.error());
        return readResult;
    }

    return tryFinishHandshake();
}

std::expected<void, uv::Error> SecureSocket::tryFinishHandshake() noexcept
{
    assert(_state == State::Handshaking);
    assert(_onHandshakeCallback.has_value());

    const int status = SSL_do_handshake(_ssl.get());

    std::expected<void, uv::Error> flushResult = flushToTransport();
    if (!flushResult.has_value()) {
        failWithError(flushResult.error());
        return flushResult;
    }

    if (status != 1) {
        const int sslError = SSL_get_error(_ssl.get(), status);

        if (sslError == SSL_ERROR_WANT_WRITE) {
            return tryFinishHandshake();  // try again
        } else if (sslError == SSL_ERROR_WANT_READ) {
            return {};  // try again later
        } else {
            failWithError(uv::Error {UV_EPROTO});
            return std::unexpected {uv::Error {UV_EPROTO}};
        }
    }

    assert(status == 1);

    _state = State::Established;

    (*_onHandshakeCallback)({});
    _onHandshakeCallback.reset();

    // We might already have data in the SSL buffers that we need to read or write.

    flushResult = flushToApplication();
    if (!flushResult.has_value()) {
        return flushResult;
    }

    return processPendingWrites();
}

std::expected<void, uv::Error> SecureSocket::tryFinishShutdown() noexcept
{
    assert(_state == State::Closing);
    assert(_onShutdownCallback.has_value());
    assert(_pendingWrites.empty());

    const int status = SSL_shutdown(_ssl.get());

    std::expected<void, uv::Error> flushResult = flushToTransport();
    if (!flushResult.has_value()) {
        failWithError(flushResult.error());
        return flushResult;
    }

    if (status == 0) {
        // Shutdown sent but we haven't received the response from the other peer yet. Try again later.
        return {};
    }

    if (status < 0) {
        const int sslError = SSL_get_error(_ssl.get(), status);

        if (sslError == SSL_ERROR_WANT_WRITE) {
            return tryFinishShutdown();  // try again
        } else if (sslError == SSL_ERROR_WANT_READ) {
            return {};  // try again later
        } else {
            failWithError(uv::Error {UV_EPROTO});
            return std::unexpected {uv::Error {UV_EPROTO}};
        }
    }

    assert(status == 1);

    _state = State::Closed;

    // SSL shutdown completed, shut down the transport.

    _transport.readStop();

    std::expected<uv::ShutdownRequest, uv::Error> shutdownResult = _transport.shutdown(std::move(*_onShutdownCallback));
    _onShutdownCallback.reset();

    if (!shutdownResult.has_value()) {
        return std::unexpected {shutdownResult.error()};
    }

    return {};
}

std::expected<void, uv::Error> SecureSocket::flushToApplication() noexcept
{
    std::array<uint8_t, defaultDecryptChunkSize> buffer {};

    while (_onReadCallback.has_value()) {  // Stops if a callback calls readStop().
        const int readCount = SSL_read(_ssl.get(), buffer.data(), static_cast<int>(buffer.size()));
        if (readCount <= 0) {
            const int sslError = SSL_get_error(_ssl.get(), readCount);
            if (sslError == SSL_ERROR_WANT_READ || sslError == SSL_ERROR_WANT_WRITE) {
                return {};
            } else if (sslError == SSL_ERROR_ZERO_RETURN) {
                // Peer requested shutdown.
                if (_onReadCallback.has_value()) {
                    (*_onReadCallback)(std::unexpected {uv::Error {UV_EOF}});
                    _onReadCallback.reset();
                }
                return {};
            } else {
                failWithError(uv::Error {UV_EPROTO});
                return std::unexpected {uv::Error {UV_EPROTO}};
            }
        }

        (*_onReadCallback)(std::span<const uint8_t> {buffer.data(), static_cast<size_t>(readCount)});
    }

    return {};
}

std::expected<void, uv::Error> SecureSocket::flushToTransport() noexcept
{
    for (;;) {
        const size_t pending = BIO_ctrl_pending(_writeBIO.get());
        if (pending == 0) {
            return {};
        }

        auto buffer = std::make_unique<std::vector<uint8_t>>(pending);

        const int readCount =
            BIO_read(_writeBIO.get(), reinterpret_cast<char*>(buffer->data()), static_cast<int>(buffer->size()));
        if (readCount <= 0) {
            return {};
        }

        buffer->resize(static_cast<size_t>(readCount));

        const std::span<const uint8_t> bufferSpan {*buffer};

        // Callback captures the buffer to extend its lifetime until the write completes
        auto writeResult = _transport.write(
            bufferSpan, [buffer = std::move(buffer)](std::expected<void, uv::Error>) mutable noexcept {});
        if (!writeResult.has_value()) {
            failWithError(writeResult.error());
            return std::unexpected {writeResult.error()};
        }
    }
}

std::expected<void, uv::Error> SecureSocket::processPendingWrites() noexcept
{
    if (_state != State::Established && _state != State::Closing) {
        return {};
    }

    while (!_pendingWrites.empty()) {
        PendingWrite& pendingWrite             = _pendingWrites.front();
        const std::span<const uint8_t> payload = pendingWrite._payload;

        if (payload.empty()) {
            pendingWrite._callback({});
            _pendingWrites.pop_front();
            continue;
        }

        // SSL only supports writes up to INT_MAX.
        const int writeSize = static_cast<int>(std::min(payload.size(), static_cast<size_t>(INT_MAX)));

        const int status = SSL_write(_ssl.get(), payload.data(), writeSize);

        if (status <= 0) {
            const int sslError = SSL_get_error(_ssl.get(), status);
            if (sslError == SSL_ERROR_WANT_READ) {
                break;
            } else if (sslError == SSL_ERROR_WANT_WRITE) {
                // SSL needs to write to transport. Flush then retry.
                std::expected<void, uv::Error> flushResult = flushToTransport();
                if (!flushResult.has_value()) {
                    return flushResult;
                }
                continue;
            } else {
                failWithError(uv::Error {UV_EPROTO});
                return std::unexpected {uv::Error {UV_EPROTO}};
            }
        }

        const size_t bytesWritten = static_cast<size_t>(status);

        if (bytesWritten >= payload.size()) {
            pendingWrite._callback({});
            _pendingWrites.pop_front();
        } else {
            pendingWrite._payload = payload.subspan(bytesWritten);
        }
    }

    std::expected<void, uv::Error> flushResult = flushToTransport();
    if (!flushResult.has_value()) {
        return flushResult;
    }

    // All writes drained, proceed the shutdown process.
    if (_state == State::Closing) {
        return tryFinishShutdown();
    }

    return {};
}
void SecureSocket::failWithError(uv::Error error) noexcept
{
    _state = State::Closed;

    _transport.readStop();

    if (_onReadCallback.has_value()) {
        (*_onReadCallback)(std::unexpected {error});
        _onReadCallback.reset();
    }

    for (PendingWrite& pendingWrite: _pendingWrites) {
        pendingWrite._callback(std::unexpected {error});
    }
    _pendingWrites.clear();

    if (_onHandshakeCallback.has_value()) {
        (*_onHandshakeCallback)(std::unexpected {error});
        _onHandshakeCallback.reset();
    }

    if (_onShutdownCallback.has_value()) {
        (*_onShutdownCallback)(std::unexpected {error});
        _onShutdownCallback.reset();
    }
}

void SecureSocket::onTransportConnected(std::expected<void, uv::Error> result) noexcept
{
    assert(_state == State::Connecting);
    assert(_onHandshakeCallback.has_value());

    if (!result.has_value()) {
        failWithError(result.error());
        return;
    }

    startHandshake(HandshakeMode::Connect);
}

void SecureSocket::onTransportRead(std::expected<std::span<const uint8_t>, uv::Error> result) noexcept
{
    if (!result.has_value()) {
        failWithError(result.error());
        return;
    }

    const std::span<const uint8_t> data = result.value();
    BIO_write(_readBIO.get(), reinterpret_cast<const char*>(data.data()), static_cast<int>(data.size()));

    std::expected<void, uv::Error> flushResult = flushToApplication();
    if (!flushResult.has_value()) {
        return;
    }

    switch (_state) {
        case State::Handshaking: {
            std::expected<void, uv::Error> handshakeResult = tryFinishHandshake();
            if (!handshakeResult.has_value()) {
                return;
            }
            break;
        }
        case State::Closing: {
            std::expected<void, uv::Error> shutdownResult = tryFinishShutdown();
            if (!shutdownResult.has_value()) {
                return;
            }
            break;
        }
        case State::Established: {
            std::expected<void, uv::Error> writeResult = processPendingWrites();
            if (!writeResult.has_value()) {
                return;
            }
            break;
        }
        default: std::unreachable();
    }
}

}  // namespace openssl
}  // namespace wrapper
}  // namespace scaler

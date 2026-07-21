#include "scaler/wrapper/openssl/secure_socket.h"

#include <algorithm>
#include <array>
#include <cassert>
#include <climits>
#include <deque>
#include <functional>
#include <utility>
#include <vector>

namespace scaler {
namespace wrapper {
namespace openssl {

SecureSocket::State::State(
    SSLContext context, uv::TCPSocket transport, SSLPtr<SSL> ssl, SSLPtr<BIO> readBIO, SSLPtr<BIO> writeBIO) noexcept
    : _context(std::move(context))
    , _transport(std::move(transport))
    , _ssl(std::move(ssl))
    , _readBIO(std::move(readBIO))
    , _writeBIO(std::move(writeBIO))
{
}

SecureSocket::SecureSocket(std::shared_ptr<State> state) noexcept: _state(std::move(state))
{
}

SecureSocket::~SecureSocket() noexcept
{
    if (_state == nullptr) {
        return;
    }

    if (_state->_connectionState == ConnectionState::Closed) {
        return;
    }

    failWithError(_state, uv::Error {UV_ECANCELED});
}

std::expected<SecureSocket, uv::Error> SecureSocket::init(uv::Loop& loop, SSLContext context) noexcept
{
    // Create the TCP transport socket
    std::expected<uv::TCPSocket, uv::Error> tcpSocket = uv::TCPSocket::init(loop);
    if (!tcpSocket.has_value()) {
        return std::unexpected {tcpSocket.error()};
    }

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

    // By default, reading from an empty BIO_s_mem() reports a hard EOF (returns 0), which OpenSSL treats as the
    // underlying transport having closed. Since the read BIO legitimately runs dry between network reads (we
    // haven't received the peer's next flight yet), tell it to report "no data yet, retry" (-1) instead of EOF.
    BIO_set_mem_eof_return(readBIO.get(), -1);

    // Associate BIOs with the SSL session.
    BIO_up_ref(readBIO.get());  // BIO_up_ref() is required as SSL_set_bio() takes ownership of these buffers.
    BIO_up_ref(writeBIO.get());
    SSL_set_bio(ssl.get(), readBIO.get(), writeBIO.get());

    auto state = std::make_shared<State>(
        std::move(context), std::move(tcpSocket.value()), std::move(ssl), std::move(readBIO), std::move(writeBIO));

    return SecureSocket {std::move(state)};
}

std::expected<uv::ConnectRequest, uv::Error> SecureSocket::connect(
    const uv::SocketAddress& address, uv::ConnectCallback callback) noexcept
{
    if (_state->_connectionState != ConnectionState::Uninitialized) {
        return std::unexpected {uv::Error {UV_EINVAL}};
    }

    _state->_connectionState     = ConnectionState::Connecting;
    _state->_onHandshakeCallback = std::move(callback);

    std::expected<uv::ConnectRequest, uv::Error> result =
        _state->_transport.connect(address, std::bind_front(&SecureSocket::onTransportConnected, _state));
    if (!result.has_value()) {
        failWithError(_state, result.error());
        return result;
    }

    return result;
}

std::expected<void, uv::Error> SecureSocket::accept(uv::ConnectCallback callback) noexcept
{
    if (_state->_connectionState != ConnectionState::Uninitialized) {
        return std::unexpected {uv::Error {UV_EINVAL}};
    }

    _state->_onHandshakeCallback = std::move(callback);

    return startHandshake(_state, HandshakeMode::Accept);
}

std::expected<void, uv::Error> SecureSocket::readStart(uv::ReadCallback callback) noexcept
{
    if (_state->_connectionState == ConnectionState::Closing || _state->_connectionState == ConnectionState::Closed) {
        return std::unexpected {uv::Error {UV_EPIPE}};
    }

    // We do not propagate readStart() to the transport layer, as we already enabled it during the SSL handshake.
    // We instead "mask" and queue the reads until the application layer registers a callback.

    _state->_onReadCallback = std::move(callback);

    return flushToApplication(_state);
}

void SecureSocket::readStop() noexcept
{
    _state->_onReadCallback.reset();
}

std::expected<void, uv::Error> SecureSocket::write(
    std::span<const std::span<const uint8_t>> buffers, uv::WriteCallback callback) noexcept
{
    if (_state->_connectionState == ConnectionState::Closing || _state->_connectionState == ConnectionState::Closed) {
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

        _state->_pendingWrites.push_back(PendingWrite {buffer, std::move(bufferCallback)});
    }

    return processPendingWrites(_state);
}

std::expected<void, uv::Error> SecureSocket::write(std::span<const uint8_t> buffer, uv::WriteCallback callback) noexcept
{
    const std::span<const std::span<const uint8_t>> buffers {&buffer, 1};
    return write(buffers, std::move(callback));
}

std::expected<void, uv::Error> SecureSocket::shutdown(uv::ShutdownCallback callback) noexcept
{
    if (_state->_connectionState == ConnectionState::Closing || _state->_connectionState == ConnectionState::Closed) {
        return std::unexpected {uv::Error {UV_EPIPE}};
    }

    _state->_connectionState    = ConnectionState::Closing;
    _state->_onShutdownCallback = std::move(callback);

    // Drain pending writes first. We will initiate the SSL shutdown once the write queue is empty.
    return processPendingWrites(_state);
}

std::expected<void, uv::Error> SecureSocket::closeReset() noexcept
{
    if (_state->_connectionState == ConnectionState::Closed) {
        return std::unexpected {uv::Error {UV_EINVAL}};
    }

    // Follow libuv behavior on close-reset, by stopping reads and calling remaining callbacks with UV_ECANCELED.
    readStop();
    failWithError(_state, uv::Error {UV_ECANCELED});

    return _state->_transport.closeReset();
}

std::expected<uv::SocketAddress, uv::Error> SecureSocket::getSockName() const noexcept
{
    return _state->_transport.getSockName();
}

std::expected<uv::SocketAddress, uv::Error> SecureSocket::getPeerName() const noexcept
{
    return _state->_transport.getPeerName();
}

std::expected<void, uv::Error> SecureSocket::nodelay(bool enable) noexcept
{
    return _state->_transport.nodelay(enable);
}

SecureSocket::ConnectionState SecureSocket::state() const noexcept
{
    return _state->_connectionState;
}

bool SecureSocket::established() const noexcept
{
    return _state->_connectionState == ConnectionState::Established;
}

uv::TCPSocket& SecureSocket::transport() noexcept
{
    return _state->_transport;
}

std::expected<void, uv::Error> SecureSocket::startHandshake(std::shared_ptr<State> state, HandshakeMode mode) noexcept
{
    assert(
        state->_connectionState == ConnectionState::Uninitialized ||
        state->_connectionState == ConnectionState::Connecting);
    assert(state->_onHandshakeCallback.has_value());

    state->_connectionState = ConnectionState::Handshaking;

    if (mode == HandshakeMode::Connect) {
        SSL_set_connect_state(state->_ssl.get());
    } else {
        SSL_set_accept_state(state->_ssl.get());
    }

    std::expected<void, uv::Error> readResult =
        state->_transport.readStart(std::bind_front(&SecureSocket::onTransportRead, state));
    if (!readResult.has_value()) {
        failWithError(state, readResult.error());
        return readResult;
    }

    return tryFinishHandshake(state);
}

std::expected<void, uv::Error> SecureSocket::tryFinishHandshake(std::shared_ptr<State> state) noexcept
{
    assert(state->_connectionState == ConnectionState::Handshaking);
    assert(state->_onHandshakeCallback.has_value());

    const int status = SSL_do_handshake(state->_ssl.get());

    std::expected<void, uv::Error> flushResult = flushToTransport(state);
    if (!flushResult.has_value()) {
        failWithError(state, flushResult.error());
        return flushResult;
    }

    if (status != 1) {
        const int sslError = SSL_get_error(state->_ssl.get(), status);

        if (sslError == SSL_ERROR_WANT_WRITE) {
            return tryFinishHandshake(state);  // try again
        } else if (sslError == SSL_ERROR_WANT_READ) {
            return {};  // try again later
        } else {
            failWithError(state, uv::Error {UV_EPROTO});
            return std::unexpected {uv::Error {UV_EPROTO}};
        }
    }

    assert(status == 1);

    state->_connectionState = ConnectionState::Established;

    (*state->_onHandshakeCallback)({});
    state->_onHandshakeCallback.reset();

    // We might already have data in the SSL buffers that we need to read or write.

    flushResult = flushToApplication(state);
    if (!flushResult.has_value()) {
        return flushResult;
    }

    return processPendingWrites(state);
}

std::expected<void, uv::Error> SecureSocket::tryFinishShutdown(std::shared_ptr<State> state) noexcept
{
    assert(state->_connectionState == ConnectionState::Closing);
    assert(state->_onShutdownCallback.has_value());
    assert(state->_pendingWrites.empty());

    const int status = SSL_shutdown(state->_ssl.get());

    std::expected<void, uv::Error> flushResult = flushToTransport(state);
    if (!flushResult.has_value()) {
        failWithError(state, flushResult.error());
        return flushResult;
    }

    if (status == 0) {
        // Shutdown sent but we haven't received the response from the other peer yet. Try again later.
        return {};
    }

    if (status < 0) {
        const int sslError = SSL_get_error(state->_ssl.get(), status);

        if (sslError == SSL_ERROR_WANT_WRITE) {
            return tryFinishShutdown(state);  // try again
        } else if (sslError == SSL_ERROR_WANT_READ) {
            return {};  // try again later
        } else {
            failWithError(state, uv::Error {UV_EPROTO});
            return std::unexpected {uv::Error {UV_EPROTO}};
        }
    }

    assert(status == 1);

    // SSL shutdown completed, shut down the transport.

    state->_transport.readStop();

    std::expected<uv::ShutdownRequest, uv::Error> shutdownResult =
        state->_transport.shutdown([state](std::expected<void, uv::Error> result) mutable {
            state->_connectionState = ConnectionState::Closed;

            if (state->_onShutdownCallback.has_value()) {
                (*state->_onShutdownCallback)(std::move(result));
                state->_onShutdownCallback.reset();
            }
        });

    if (!shutdownResult.has_value()) {
        return std::unexpected {shutdownResult.error()};
    }

    return {};
}

std::expected<void, uv::Error> SecureSocket::flushToApplication(std::shared_ptr<State> state) noexcept
{
    std::array<uint8_t, defaultDecryptChunkSize> buffer {};

    // When closing, we must drain and discard any pending application data from the SSL buffer even if no read callback
    // is registered, or else OpenSSL cannot complete its shutdown process.
    const bool isClosing = (state->_connectionState == ConnectionState::Closing);

    while (state->_onReadCallback.has_value() || isClosing) {
        const int readCount = SSL_read(state->_ssl.get(), buffer.data(), static_cast<int>(buffer.size()));
        if (readCount <= 0) {
            const int sslError = SSL_get_error(state->_ssl.get(), readCount);
            if (sslError == SSL_ERROR_WANT_READ || sslError == SSL_ERROR_WANT_WRITE) {
                return {};
            } else if (sslError == SSL_ERROR_ZERO_RETURN) {
                // Peer requested shutdown.
                if (state->_onReadCallback.has_value()) {
                    (*state->_onReadCallback)(std::unexpected {uv::Error {UV_EOF}});
                    state->_onReadCallback.reset();
                }
                return {};
            } else {
                failWithError(state, uv::Error {UV_EPROTO});
                return std::unexpected {uv::Error {UV_EPROTO}};
            }
        }

        if (state->_onReadCallback.has_value()) {
            (*state->_onReadCallback)(std::span<const uint8_t> {buffer.data(), static_cast<size_t>(readCount)});
        }
    }

    return {};
}

std::expected<void, uv::Error> SecureSocket::flushToTransport(std::shared_ptr<State> state) noexcept
{
    for (;;) {
        const size_t pending = BIO_ctrl_pending(state->_writeBIO.get());
        if (pending == 0) {
            return {};
        }

        auto buffer = std::make_unique<std::vector<uint8_t>>(pending);

        const int readCount =
            BIO_read(state->_writeBIO.get(), reinterpret_cast<char*>(buffer->data()), static_cast<int>(buffer->size()));
        if (readCount <= 0) {
            return {};
        }

        buffer->resize(static_cast<size_t>(readCount));

        const std::span<const uint8_t> bufferSpan {*buffer};

        // Callback captures the buffer to extend its lifetime until the write completes
        auto writeResult = state->_transport.write(
            bufferSpan, [buffer = std::move(buffer)](std::expected<void, uv::Error>) mutable noexcept {});
        if (!writeResult.has_value()) {
            failWithError(state, writeResult.error());
            return std::unexpected {writeResult.error()};
        }
    }
}

std::expected<void, uv::Error> SecureSocket::processPendingWrites(std::shared_ptr<State> state) noexcept
{
    if (state->_connectionState != ConnectionState::Established &&
        state->_connectionState != ConnectionState::Closing) {
        return {};
    }

    while (!state->_pendingWrites.empty()) {
        PendingWrite& pendingWrite             = state->_pendingWrites.front();
        const std::span<const uint8_t> payload = pendingWrite._payload;

        if (payload.empty()) {
            auto callback = std::move(pendingWrite._callback);
            state->_pendingWrites.pop_front();
            callback({});
            continue;
        }

        // SSL only supports writes up to INT_MAX.
        const int writeSize = static_cast<int>(std::min(payload.size(), static_cast<size_t>(INT_MAX)));

        const int status = SSL_write(state->_ssl.get(), payload.data(), writeSize);

        if (status <= 0) {
            const int sslError = SSL_get_error(state->_ssl.get(), status);
            if (sslError == SSL_ERROR_WANT_READ) {
                break;
            } else if (sslError == SSL_ERROR_WANT_WRITE) {
                // SSL needs to write to transport. Flush then retry.
                std::expected<void, uv::Error> flushResult = flushToTransport(state);
                if (!flushResult.has_value()) {
                    return flushResult;
                }
                continue;
            } else {
                failWithError(state, uv::Error {UV_EPROTO});
                return std::unexpected {uv::Error {UV_EPROTO}};
            }
        }

        const size_t bytesWritten = static_cast<size_t>(status);

        if (bytesWritten >= payload.size()) {
            auto callback = std::move(pendingWrite._callback);
            state->_pendingWrites.pop_front();
            callback({});
        } else {
            pendingWrite._payload = payload.subspan(bytesWritten);
        }
    }

    std::expected<void, uv::Error> flushResult = flushToTransport(state);
    if (!flushResult.has_value()) {
        return flushResult;
    }

    // All writes drained, proceed the shutdown process.
    if (state->_connectionState == ConnectionState::Closing) {
        return tryFinishShutdown(state);
    }

    return {};
}

void SecureSocket::failWithError(std::shared_ptr<State> state, uv::Error error) noexcept
{
    state->_connectionState = ConnectionState::Closed;

    state->_transport.readStop();

    if (state->_onReadCallback.has_value()) {
        auto callback = std::move(*state->_onReadCallback);
        state->_onReadCallback.reset();
        callback(std::unexpected {error});
    }

    std::deque<PendingWrite> pendingWrites = std::move(state->_pendingWrites);
    state->_pendingWrites.clear();
    for (PendingWrite& pendingWrite: pendingWrites) {
        pendingWrite._callback(std::unexpected {error});
    }

    if (state->_onHandshakeCallback.has_value()) {
        auto callback = std::move(*state->_onHandshakeCallback);
        state->_onHandshakeCallback.reset();
        callback(std::unexpected {error});
    }

    if (state->_onShutdownCallback.has_value()) {
        auto callback = std::move(*state->_onShutdownCallback);
        state->_onShutdownCallback.reset();
        callback(std::unexpected {error});
    }
}

void SecureSocket::onTransportConnected(std::shared_ptr<State> state, std::expected<void, uv::Error> result) noexcept
{
    assert(state->_connectionState == ConnectionState::Connecting);
    assert(state->_onHandshakeCallback.has_value());

    if (!result.has_value()) {
        failWithError(state, result.error());
        return;
    }

    startHandshake(state, HandshakeMode::Connect);
}

void SecureSocket::onTransportRead(
    std::shared_ptr<State> state, std::expected<std::span<const uint8_t>, uv::Error> result) noexcept
{
    if (!result.has_value()) {
        failWithError(state, result.error());
        return;
    }

    const std::span<const uint8_t> data = result.value();
    BIO_write(state->_readBIO.get(), reinterpret_cast<const char*>(data.data()), static_cast<int>(data.size()));

    std::expected<void, uv::Error> flushResult = flushToApplication(state);
    if (!flushResult.has_value()) {
        return;
    }

    switch (state->_connectionState) {
        case ConnectionState::Handshaking: {
            std::expected<void, uv::Error> handshakeResult = tryFinishHandshake(state);
            if (!handshakeResult.has_value()) {
                return;
            }
            break;
        }
        case ConnectionState::Closing: {
            std::expected<void, uv::Error> shutdownResult = tryFinishShutdown(state);
            if (!shutdownResult.has_value()) {
                return;
            }
            break;
        }
        case ConnectionState::Established: {
            std::expected<void, uv::Error> writeResult = processPendingWrites(state);
            if (!writeResult.has_value()) {
                return;
            }
            break;
        }
        case ConnectionState::Closed: return;
        default: std::unreachable();
    }
}

}  // namespace openssl
}  // namespace wrapper
}  // namespace scaler

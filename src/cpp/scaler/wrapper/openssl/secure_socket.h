#pragma once

#include <openssl/bio.h>
#include <openssl/ssl.h>

#include <cstddef>
#include <cstdint>
#include <deque>
#include <expected>
#include <memory>
#include <optional>
#include <span>

#include "scaler/wrapper/openssl/ssl_context.h"
#include "scaler/wrapper/uv/callback.h"
#include "scaler/wrapper/uv/loop.h"
#include "scaler/wrapper/uv/socket_address.h"
#include "scaler/wrapper/uv/tcp.h"

namespace scaler {
namespace wrapper {
namespace openssl {

// A libuv-like socket implementing SSL/TLS using OpenSSL.
class SecureSocket {
public:
    enum class State {
        Uninitialized,
        Connecting,
        Handshaking,
        Established,
        Closing,
        Closed,
    };

    static std::expected<SecureSocket, uv::Error> init(SSLContext context, uv::TCPSocket transport) noexcept;

    // Initiate a client-side TLS connection.
    std::expected<uv::ConnectRequest, uv::Error> connect(
        const uv::SocketAddress& address, uv::ConnectCallback callback) noexcept;

    // Initiate a server-side TLS handshake.
    // The callback is called when the handshake completes (or fails).
    std::expected<void, uv::Error> accept(uv::ConnectCallback callback) noexcept;

    std::expected<void, uv::Error> readStart(uv::ReadCallback callback) noexcept;

    void readStop() noexcept;

    std::expected<void, uv::Error> write(
        std::span<const std::span<const uint8_t>> buffers, uv::WriteCallback callback) noexcept;

    std::expected<void, uv::Error> write(std::span<const uint8_t> buffer, uv::WriteCallback callback) noexcept;

    std::expected<void, uv::Error> shutdown(uv::ShutdownCallback callback) noexcept;

    std::expected<void, uv::Error> closeReset() noexcept;

    std::expected<uv::SocketAddress, uv::Error> getSockName() const noexcept;

    std::expected<uv::SocketAddress, uv::Error> getPeerName() const noexcept;

    std::expected<void, uv::Error> nodelay(bool enable) noexcept;

    State state() const noexcept;

    bool established() const noexcept;

    uv::TCPSocket& transport() noexcept;

private:
    static constexpr size_t defaultDecryptChunkSize = 16 * 1024;

    enum class HandshakeMode { Connect, Accept };

    struct PendingWrite {
        std::span<const uint8_t> _payload;
        uv::WriteCallback _callback;
    };

    SecureSocket(
        SSLContext context,
        uv::TCPSocket transport,
        SSLPtr<SSL> ssl,
        SSLPtr<BIO> readBIO,
        SSLPtr<BIO> writeBIO) noexcept;

    std::expected<void, uv::Error> startHandshake(HandshakeMode mode) noexcept;

    std::expected<void, uv::Error> tryFinishHandshake() noexcept;

    std::expected<void, uv::Error> tryFinishShutdown() noexcept;

    std::expected<void, uv::Error> flushToApplication() noexcept;

    std::expected<void, uv::Error> flushToTransport() noexcept;

    std::expected<void, uv::Error> processPendingWrites() noexcept;

    void failWithError(uv::Error error) noexcept;

    void onTransportConnected(std::expected<void, uv::Error> result) noexcept;

    void onTransportRead(std::expected<std::span<const uint8_t>, uv::Error> result) noexcept;

    SSLContext _context;
    uv::TCPSocket _transport;

    SSLPtr<SSL> _ssl {};
    SSLPtr<BIO> _readBIO {};
    SSLPtr<BIO> _writeBIO {};

    State _state {State::Uninitialized};

    std::optional<uv::ConnectCallback> _onHandshakeCallback {};
    std::optional<uv::ReadCallback> _onReadCallback {};
    std::optional<uv::ShutdownCallback> _onShutdownCallback {};

    std::deque<PendingWrite> _pendingWrites {};
};

}  // namespace openssl
}  // namespace wrapper
}  // namespace scaler

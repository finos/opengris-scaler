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
    enum class ConnectionState {
        Uninitialized,
        Connecting,
        Handshaking,
        Established,
        Closing,
        Closed,
    };

    ~SecureSocket() noexcept;

    SecureSocket(const SecureSocket&)            = delete;
    SecureSocket& operator=(const SecureSocket&) = delete;

    SecureSocket(SecureSocket&&) noexcept            = default;
    SecureSocket& operator=(SecureSocket&&) noexcept = default;

    static std::expected<SecureSocket, uv::Error> init(uv::Loop& loop, SSLContext context) noexcept;

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

    ConnectionState state() const noexcept;

    bool established() const noexcept;

    uv::TCPSocket& transport() noexcept;

private:
    static constexpr size_t defaultDecryptChunkSize = 16 * 1024;

    enum class HandshakeMode { Connect, Accept };

    struct PendingWrite {
        std::span<const uint8_t> _payload;
        uv::WriteCallback _callback;
    };

    // State is heap-allocated to provide a stable memory for callbacks if the socket is std::move'd.

    struct State {
        SSLContext _context;
        uv::TCPSocket _transport;

        SSLPtr<SSL> _ssl {};
        SSLPtr<BIO> _readBIO {};
        SSLPtr<BIO> _writeBIO {};

        ConnectionState _connectionState {ConnectionState::Uninitialized};

        std::optional<uv::ConnectCallback> _onHandshakeCallback {};
        std::optional<uv::ReadCallback> _onReadCallback {};
        std::optional<uv::ShutdownCallback> _onShutdownCallback {};

        std::deque<PendingWrite> _pendingWrites {};

        State(
            SSLContext context,
            uv::TCPSocket transport,
            SSLPtr<SSL> ssl,
            SSLPtr<BIO> readBIO,
            SSLPtr<BIO> writeBIO) noexcept;
    };

    SecureSocket(std::shared_ptr<State> state) noexcept;

    std::shared_ptr<State> _state;

    static std::expected<void, uv::Error> startHandshake(std::shared_ptr<State> state, HandshakeMode mode) noexcept;

    static std::expected<void, uv::Error> tryFinishHandshake(std::shared_ptr<State> state) noexcept;

    static std::expected<void, uv::Error> tryFinishShutdown(std::shared_ptr<State> state) noexcept;

    static std::expected<void, uv::Error> flushToApplication(std::shared_ptr<State> state) noexcept;

    static std::expected<void, uv::Error> flushToTransport(std::shared_ptr<State> state) noexcept;

    static std::expected<void, uv::Error> processPendingWrites(std::shared_ptr<State> state) noexcept;

    static void failWithError(std::shared_ptr<State> state, uv::Error error) noexcept;

    static void onTransportConnected(std::shared_ptr<State> state, std::expected<void, uv::Error> result) noexcept;

    static void onTransportRead(
        std::shared_ptr<State> state, std::expected<std::span<const uint8_t>, uv::Error> result) noexcept;
};

}  // namespace openssl
}  // namespace wrapper
}  // namespace scaler

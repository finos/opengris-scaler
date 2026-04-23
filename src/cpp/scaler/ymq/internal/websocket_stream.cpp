#include "scaler/ymq/internal/websocket_stream.h"

#include <cstdint>
#include <cstring>
#include <expected>
#include <memory>
#include <optional>
#include <random>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "scaler/ymq/internal/websocket_utils.h"

namespace scaler {
namespace ymq {
namespace internal {

namespace {

static constexpr uint64_t kMaxWebSocketPayloadSize = 64ULL * 1024 * 1024;  // 64 MB
static constexpr size_t kMaxUpgradeHeaderSize      = 64 * 1024;            // 64 KB

std::string buildClientUpgradeRequest(
    const std::string& host, uint16_t port, const std::string& path, const std::string& key) noexcept
{
    return "GET " + path +
           " HTTP/1.1\r\n"
           "Host: " +
           host + ":" + std::to_string(port) +
           "\r\n"
           "Upgrade: websocket\r\n"
           "Connection: Upgrade\r\n"
           "Sec-WebSocket-Key: " +
           key +
           "\r\n"
           "Sec-WebSocket-Version: 13\r\n"
           "\r\n";
}

std::string buildServerUpgradeResponse(const std::string& key) noexcept
{
    return "HTTP/1.1 101 Switching Protocols\r\n"
           "Upgrade: websocket\r\n"
           "Connection: Upgrade\r\n"
           "Sec-WebSocket-Accept: " +
           computeWebSocketAccept(key) +
           "\r\n"
           "\r\n";
}

// ─── WebSocket frame encoding ────────────────────────────────────────────────

// Returns the frame header bytes for a server-side frame (unmasked).
std::vector<uint8_t> buildServerFrameHeader(size_t payloadSize) noexcept
{
    std::vector<uint8_t> header;
    header.push_back(0x82);  // FIN | opcode=binary
    if (payloadSize < 126) {
        header.push_back(static_cast<uint8_t>(payloadSize));
    } else if (payloadSize < 65536) {
        header.push_back(126);
        header.push_back(static_cast<uint8_t>((payloadSize >> 8) & 0xFF));
        header.push_back(static_cast<uint8_t>(payloadSize & 0xFF));
    } else {
        header.push_back(127);
        for (int i = 7; i >= 0; --i)
            header.push_back(static_cast<uint8_t>((payloadSize >> (i * 8)) & 0xFF));
    }
    return header;
}

// Returns {header+mask, masked-payload-copy} for a client-side frame (masked, RFC 6455 §5.3).
std::pair<std::vector<uint8_t>, std::vector<uint8_t>> buildClientFrame(
    std::span<const std::span<const uint8_t>> buffers, size_t totalSize) noexcept
{
    static thread_local std::mt19937 rng(std::random_device {}());
    std::uniform_int_distribution<uint32_t> dist;
    std::array<uint8_t, 4> maskKey;
    const uint32_t maskInt = dist(rng);
    std::memcpy(maskKey.data(), &maskInt, 4);

    std::vector<uint8_t> header;
    header.push_back(0x82);  // FIN | binary
    if (totalSize < 126) {
        header.push_back(0x80 | static_cast<uint8_t>(totalSize));
    } else if (totalSize < 65536) {
        header.push_back(0x80 | 126);
        header.push_back(static_cast<uint8_t>((totalSize >> 8) & 0xFF));
        header.push_back(static_cast<uint8_t>(totalSize & 0xFF));
    } else {
        header.push_back(0x80 | 127);
        for (int i = 7; i >= 0; --i)
            header.push_back(static_cast<uint8_t>((totalSize >> (i * 8)) & 0xFF));
    }
    header.insert(header.end(), maskKey.begin(), maskKey.end());

    std::vector<uint8_t> masked;
    masked.reserve(totalSize);
    size_t pos = 0;
    for (const auto& buf: buffers) {
        for (uint8_t byte: buf)
            masked.push_back(byte ^ maskKey[pos++ % 4]);
    }
    return {std::move(header), std::move(masked)};
}

// Builds a control frame (CLOSE=0x8, PING=0x9, PONG=0xA). RFC 6455 §5.5:
// control frames are always FIN=1 and carry at most 125 bytes of payload.
// Client frames must be masked; server frames must not.
std::vector<uint8_t> buildControlFrame(uint8_t opcode, bool isClient, std::span<const uint8_t> payload) noexcept
{
    static thread_local std::mt19937 rng(std::random_device {}());
    std::uniform_int_distribution<uint32_t> dist;

    const uint8_t len = static_cast<uint8_t>(payload.size());  // caller ensures ≤ 125

    std::vector<uint8_t> frame;
    frame.push_back(0x80 | opcode);  // FIN=1
    if (isClient) {
        frame.push_back(0x80 | len);  // MASK=1
        std::array<uint8_t, 4> maskKey;
        const uint32_t maskInt = dist(rng);
        std::memcpy(maskKey.data(), &maskInt, 4);
        frame.insert(frame.end(), maskKey.begin(), maskKey.end());
        for (size_t i = 0; i < payload.size(); ++i)
            frame.push_back(payload[i] ^ maskKey[i % 4]);
    } else {
        frame.push_back(len);
        frame.insert(frame.end(), payload.begin(), payload.end());
    }
    return frame;
}

// ─── Frame decoding ──────────────────────────────────────────────────────────

struct DecodedFrame {
    uint8_t opcode;
    bool fin;
    std::vector<uint8_t> payload;
};

// Tries to parse one complete WebSocket frame from buffer, consuming it in-place.
//   unexpected(error) — protocol error (payload exceeds kMaxWebSocketPayloadSize)
//   {nullopt}         — buffer does not yet contain a full frame
//   {DecodedFrame}    — one frame decoded and consumed
std::expected<std::optional<DecodedFrame>, scaler::wrapper::uv::Error> tryDecodeFrame(
    std::vector<uint8_t>& buffer) noexcept
{
    if (buffer.size() < 2)
        return std::optional<DecodedFrame> {std::nullopt};

    const uint8_t byte0  = buffer[0];
    const uint8_t byte1  = buffer[1];
    const bool fin       = (byte0 & 0x80) != 0;
    const uint8_t opcode = byte0 & 0x0F;
    const bool masked    = (byte1 & 0x80) != 0;
    uint64_t payloadLen  = byte1 & 0x7F;
    size_t headerSize    = 2;

    if (payloadLen == 126) {
        if (buffer.size() < 4)
            return std::optional<DecodedFrame> {std::nullopt};
        payloadLen = (uint64_t(buffer[2]) << 8) | buffer[3];
        headerSize = 4;
    } else if (payloadLen == 127) {
        if (buffer.size() < 10)
            return std::optional<DecodedFrame> {std::nullopt};
        payloadLen = 0;
        for (int i = 0; i < 8; ++i)
            payloadLen = (payloadLen << 8) | buffer[2 + i];
        headerSize = 10;
    }

    if (payloadLen > kMaxWebSocketPayloadSize)
        return std::unexpected(scaler::wrapper::uv::Error {UV_EPROTO});

    if (masked)
        headerSize += 4;

    if (buffer.size() < headerSize + static_cast<size_t>(payloadLen))
        return std::optional<DecodedFrame> {std::nullopt};

    std::vector<uint8_t> payload(static_cast<size_t>(payloadLen));
    if (masked) {
        const uint8_t* maskKey = buffer.data() + headerSize - 4;
        for (size_t i = 0; i < static_cast<size_t>(payloadLen); ++i)
            payload[i] = buffer[headerSize + i] ^ maskKey[i % 4];
    } else {
        std::copy(
            buffer.begin() + static_cast<std::ptrdiff_t>(headerSize),
            buffer.begin() + static_cast<std::ptrdiff_t>(headerSize + static_cast<size_t>(payloadLen)),
            payload.begin());
    }

    buffer.erase(
        buffer.begin(), buffer.begin() + static_cast<std::ptrdiff_t>(headerSize + static_cast<size_t>(payloadLen)));
    return std::optional<DecodedFrame> {DecodedFrame {opcode, fin, std::move(payload)}};
}

}  // anonymous namespace

// ─── Upgrade state ───────────────────────────────────────────────────────────

// Shared state used during the async HTTP upgrade phase on the client side.
// TCPSocket has no public default constructor, so we wrap it in optional.
struct ClientUpgradeContext {
    std::optional<scaler::wrapper::uv::TCPSocket> socket {};
    std::string key {};
    std::vector<uint8_t> recvBuffer {};
    scaler::utility::MoveOnlyFunction<void(std::expected<WebSocketStream, scaler::wrapper::uv::Error>)> callback {};
};

// Shared state used during the async HTTP upgrade phase on the server side.
struct ServerUpgradeContext {
    std::optional<scaler::wrapper::uv::TCPSocket> socket {};
    std::vector<uint8_t> recvBuffer {};
    scaler::utility::MoveOnlyFunction<void(std::expected<WebSocketStream, scaler::wrapper::uv::Error>)> callback {};
};

// ─── WebSocketStream implementation ─────────────────────────────────────────

WebSocketStream::State::State(scaler::wrapper::uv::TCPSocket socket, bool isServer) noexcept
    : _socket(std::move(socket)), _isServer(isServer)
{
}

WebSocketStream::WebSocketStream(std::shared_ptr<State> state) noexcept: _state(std::move(state))
{
}

WebSocketStream WebSocketStream::fromUpgradedSocket(
    scaler::wrapper::uv::TCPSocket socket, bool isServer, std::vector<uint8_t> leftover) noexcept
{
    auto state         = std::make_shared<State>(std::move(socket), isServer);
    state->_recvBuffer = std::move(leftover);
    return WebSocketStream(std::move(state));
}

// Called when a complete HTTP response has been assembled in the client upgrade context.
static void finishClientUpgrade(std::shared_ptr<ClientUpgradeContext> ctx) noexcept
{
    ctx->socket->readStop();

    const std::string_view response(reinterpret_cast<const char*>(ctx->recvBuffer.data()), ctx->recvBuffer.size());

    const size_t headersEnd = response.find("\r\n\r\n");
    if (headersEnd == std::string_view::npos) {
        ctx->callback(std::unexpected(scaler::wrapper::uv::Error {UV_EPROTO}));
        return;
    }

    const std::string_view headers = response.substr(0, headersEnd);

    if (!headers.starts_with("HTTP/1.1 101")) {
        ctx->callback(std::unexpected(scaler::wrapper::uv::Error {UV_EPROTO}));
        return;
    }

    const auto acceptHeader = extractHeader(headers, "Sec-WebSocket-Accept");
    if (!acceptHeader.has_value() || *acceptHeader != computeWebSocketAccept(ctx->key)) {
        ctx->callback(std::unexpected(scaler::wrapper::uv::Error {UV_EPROTO}));
        return;
    }

    // Preserve any data that arrived after the HTTP headers (should be empty in practice).
    std::vector<uint8_t> leftover(
        ctx->recvBuffer.begin() + static_cast<std::ptrdiff_t>(headersEnd + 4), ctx->recvBuffer.end());

    ctx->callback(
        WebSocketStream::fromUpgradedSocket(std::move(ctx->socket.value()), false /* client */, std::move(leftover)));
}

// Called when a complete HTTP request has been assembled in the server upgrade context.
static void finishServerUpgrade(std::shared_ptr<ServerUpgradeContext> ctx) noexcept
{
    ctx->socket->readStop();

    const std::string_view request(reinterpret_cast<const char*>(ctx->recvBuffer.data()), ctx->recvBuffer.size());

    const size_t headersEnd = request.find("\r\n\r\n");
    if (headersEnd == std::string_view::npos) {
        ctx->callback(std::unexpected(scaler::wrapper::uv::Error {UV_EPROTO}));
        return;
    }

    const std::string_view headers = request.substr(0, headersEnd);

    // Verify request line: must be GET <path> HTTP/1.1
    const size_t firstLineEnd = headers.find("\r\n");
    if (firstLineEnd == std::string_view::npos) {
        ctx->callback(std::unexpected(scaler::wrapper::uv::Error {UV_EPROTO}));
        return;
    }
    const std::string_view requestLine = headers.substr(0, firstLineEnd);
    if (!requestLine.starts_with("GET ") || !requestLine.ends_with(" HTTP/1.1")) {
        ctx->callback(std::unexpected(scaler::wrapper::uv::Error {UV_EPROTO}));
        return;
    }

    const auto upgradeHeader    = extractHeader(headers, "Upgrade");
    const auto keyHeader        = extractHeader(headers, "Sec-WebSocket-Key");
    const auto connectionHeader = extractHeader(headers, "Connection");
    const auto versionHeader    = extractHeader(headers, "Sec-WebSocket-Version");

    if (!upgradeHeader.has_value() || !keyHeader.has_value() || !connectionHeader.has_value() ||
        !versionHeader.has_value()) {
        ctx->callback(std::unexpected(scaler::wrapper::uv::Error {UV_EPROTO}));
        return;
    }

    // Verify Upgrade: websocket (case-insensitive)
    std::string upgradeValue = *upgradeHeader;
    for (char& c: upgradeValue)
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    if (upgradeValue != "websocket") {
        ctx->callback(std::unexpected(scaler::wrapper::uv::Error {UV_EPROTO}));
        return;
    }

    // Verify Connection header contains "upgrade" (case-insensitive, may be a token list)
    std::string connectionValue = *connectionHeader;
    for (char& c: connectionValue)
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    if (connectionValue.find("upgrade") == std::string::npos) {
        ctx->callback(std::unexpected(scaler::wrapper::uv::Error {UV_EPROTO}));
        return;
    }

    if (*versionHeader != "13") {
        ctx->callback(std::unexpected(scaler::wrapper::uv::Error {UV_EPROTO}));
        return;
    }

    const std::string response = buildServerUpgradeResponse(*keyHeader);
    auto responseData          = std::make_shared<std::string>(response);
    const std::span<const uint8_t> responseSpan(
        reinterpret_cast<const uint8_t*>(responseData->data()), responseData->size());

    auto writeResult = ctx->socket->write(
        std::span<const std::span<const uint8_t>>(&responseSpan, 1),
        [ctx, responseData = std::move(responseData)](std::expected<void, scaler::wrapper::uv::Error> result) mutable {
            if (!result.has_value()) {
                ctx->callback(std::unexpected(result.error()));
                return;
            }

            // Any data that arrived before the response write isn't expected but preserve it.
            ctx->callback(WebSocketStream::fromUpgradedSocket(std::move(ctx->socket.value()), true /* server */));
        });

    if (!writeResult.has_value()) {
        ctx->callback(std::unexpected(writeResult.error()));
    }
}

void WebSocketStream::upgradeAsClient(
    scaler::wrapper::uv::TCPSocket socket,
    const WebSocketAddress& address,
    scaler::utility::MoveOnlyFunction<void(std::expected<WebSocketStream, scaler::wrapper::uv::Error>)>
        callback) noexcept
{
    auto ctx      = std::make_shared<ClientUpgradeContext>();
    ctx->socket   = std::move(socket);
    ctx->key      = generateWebSocketKey();
    ctx->callback = std::move(callback);

    const std::string request = buildClientUpgradeRequest(address.host, address.port, address.path, ctx->key);
    auto requestData          = std::make_shared<std::string>(request);
    const std::span<const uint8_t> requestSpan(
        reinterpret_cast<const uint8_t*>(requestData->data()), requestData->size());

    auto writeResult = ctx->socket->write(
        std::span<const std::span<const uint8_t>>(&requestSpan, 1),
        [ctx, requestData = std::move(requestData)](std::expected<void, scaler::wrapper::uv::Error> result) mutable {
            if (!result.has_value()) {
                ctx->callback(std::unexpected(result.error()));
                return;
            }

            auto readStartResult = ctx->socket->readStart(
                [ctx](std::expected<std::span<const uint8_t>, scaler::wrapper::uv::Error> readResult) mutable {
                    if (!readResult.has_value()) {
                        ctx->socket->readStop();
                        ctx->callback(std::unexpected(readResult.error()));
                        return;
                    }

                    const auto& data = readResult.value();
                    ctx->recvBuffer.insert(ctx->recvBuffer.end(), data.begin(), data.end());

                    if (ctx->recvBuffer.size() > kMaxUpgradeHeaderSize) {
                        ctx->socket->readStop();
                        ctx->callback(std::unexpected(scaler::wrapper::uv::Error {UV_EPROTO}));
                        return;
                    }

                    const std::string_view view(
                        reinterpret_cast<const char*>(ctx->recvBuffer.data()), ctx->recvBuffer.size());
                    if (view.find("\r\n\r\n") != std::string_view::npos) {
                        finishClientUpgrade(std::move(ctx));
                    }
                });

            if (!readStartResult.has_value()) {
                ctx->callback(std::unexpected(readStartResult.error()));
            }
        });

    if (!writeResult.has_value()) {
        auto cb = std::move(ctx->callback);
        cb(std::unexpected(writeResult.error()));
    }
}

void WebSocketStream::upgradeAsServer(
    scaler::wrapper::uv::TCPSocket socket,
    scaler::utility::MoveOnlyFunction<void(std::expected<WebSocketStream, scaler::wrapper::uv::Error>)>
        callback) noexcept
{
    auto ctx      = std::make_shared<ServerUpgradeContext>();
    ctx->socket   = std::move(socket);
    ctx->callback = std::move(callback);

    auto readStartResult = ctx->socket->readStart(
        [ctx](std::expected<std::span<const uint8_t>, scaler::wrapper::uv::Error> readResult) mutable {
            if (!readResult.has_value()) {
                // Copy ctx to the stack before readStop() — readStop() destroys this lambda (and the
                // captured ctx) via setData({}), so ctx must outlive that call.
                auto safeCtx = ctx;
                ctx->socket->readStop();
                safeCtx->callback(std::unexpected(readResult.error()));
                return;
            }

            const auto& data = readResult.value();
            ctx->recvBuffer.insert(ctx->recvBuffer.end(), data.begin(), data.end());

            if (ctx->recvBuffer.size() > kMaxUpgradeHeaderSize) {
                auto safeCtx = ctx;
                ctx->socket->readStop();
                safeCtx->callback(std::unexpected(scaler::wrapper::uv::Error {UV_EPROTO}));
                return;
            }

            const std::string_view view(reinterpret_cast<const char*>(ctx->recvBuffer.data()), ctx->recvBuffer.size());
            if (view.find("\r\n\r\n") != std::string_view::npos) {
                finishServerUpgrade(std::move(ctx));
            }
        });

    if (!readStartResult.has_value()) {
        auto cb = std::move(ctx->callback);
        cb(std::unexpected(readStartResult.error()));
    }
}

// ─── Data path ───────────────────────────────────────────────────────────────

std::expected<void, scaler::wrapper::uv::Error> WebSocketStream::write(
    std::span<const std::span<const uint8_t>> buffers, scaler::wrapper::uv::WriteCallback callback) noexcept
{
    size_t totalSize = 0;
    for (const auto& buf: buffers)
        totalSize += buf.size();

    if (_state->_isServer) {
        auto header     = std::make_shared<std::vector<uint8_t>>(buildServerFrameHeader(totalSize));
        auto headerSpan = std::span<const uint8_t>(*header);

        std::vector<std::span<const uint8_t>> writeBuffers;
        writeBuffers.reserve(buffers.size() + 1);
        writeBuffers.push_back(headerSpan);
        for (const auto& buf: buffers)
            writeBuffers.push_back(buf);

        auto result = _state->_socket.write(
            std::span<const std::span<const uint8_t>>(writeBuffers),
            [header = std::move(header), callback = std::move(callback)](
                std::expected<void, scaler::wrapper::uv::Error> err) mutable { callback(err); });

        if (!result.has_value())
            return std::unexpected(result.error());
        return {};
    }

    auto [header, masked] = buildClientFrame(buffers, totalSize);
    auto headerData       = std::make_shared<std::vector<uint8_t>>(std::move(header));
    auto maskedData       = std::make_shared<std::vector<uint8_t>>(std::move(masked));

    const std::span<const uint8_t> headerSpan(*headerData);
    const std::span<const uint8_t> maskedSpan(*maskedData);
    const std::array<std::span<const uint8_t>, 2> writeBuffers {headerSpan, maskedSpan};

    auto result = _state->_socket.write(
        std::span<const std::span<const uint8_t>>(writeBuffers),
        [headerData = std::move(headerData), maskedData = std::move(maskedData), callback = std::move(callback)](
            std::expected<void, scaler::wrapper::uv::Error> err) mutable { callback(err); });

    if (!result.has_value())
        return std::unexpected(result.error());
    return {};
}

std::expected<void, scaler::wrapper::uv::Error> WebSocketStream::readStart(
    scaler::wrapper::uv::ReadCallback callback) noexcept
{
    auto state           = _state;
    state->_readCallback = std::move(callback);
    state->_readActive   = true;

    return state->_socket.readStart(
        [state](std::expected<std::span<const uint8_t>, scaler::wrapper::uv::Error> result) mutable {
            if (!result.has_value()) {
                if (state->_readActive && state->_readCallback) {
                    state->_readCallback(std::unexpected(result.error()));
                }
                return;
            }

            const auto& data = result.value();
            state->_recvBuffer.insert(state->_recvBuffer.end(), data.begin(), data.end());

            while (state->_readActive && !state->_recvBuffer.empty()) {
                auto frameResult = tryDecodeFrame(state->_recvBuffer);
                if (!frameResult.has_value()) {
                    state->_readCallback(std::unexpected(frameResult.error()));
                    return;
                }
                if (!frameResult->has_value())
                    break;  // need more data

                auto& frame = frameResult->value();

                if (frame.opcode == 0x8) {
                    // CLOSE: echo a CLOSE frame then signal clean disconnect.
                    auto closeFrame = buildControlFrame(0x8, !state->_isServer, {});
                    auto frameData  = std::make_shared<std::vector<uint8_t>>(std::move(closeFrame));
                    const std::span<const uint8_t> frameSpan(*frameData);
                    // Best-effort CLOSE echo — connection is shutting down regardless.
                    if (auto r = state->_socket.write(
                            std::span<const std::span<const uint8_t>>(&frameSpan, 1),
                            [frameData = std::move(frameData)](std::expected<void, scaler::wrapper::uv::Error>) {});
                        !r.has_value()) {
                    }
                    if (state->_readActive && state->_readCallback)
                        state->_readCallback(std::unexpected(scaler::wrapper::uv::Error {UV_EOF}));
                    return;
                }

                if (frame.opcode == 0x9) {
                    // PING: respond with PONG carrying the same payload (RFC 6455 §5.5.3).
                    auto pongPayload = frame.payload;
                    if (pongPayload.size() > 125)
                        pongPayload.resize(125);
                    auto pongFrame = buildControlFrame(0xA, !state->_isServer, pongPayload);
                    auto frameData = std::make_shared<std::vector<uint8_t>>(std::move(pongFrame));
                    const std::span<const uint8_t> frameSpan(*frameData);
                    // Best-effort PONG — if this write fails the next read will catch the error.
                    if (auto r = state->_socket.write(
                            std::span<const std::span<const uint8_t>>(&frameSpan, 1),
                            [frameData = std::move(frameData)](std::expected<void, scaler::wrapper::uv::Error>) {});
                        !r.has_value()) {
                    }
                    continue;
                }

                if (frame.opcode == 0xA) {
                    // PONG: unsolicited or in response to our PING — ignore.
                    continue;
                }

                // Data frames: handle fragmentation per RFC 6455 §5.4.
                if (frame.opcode == 0x1 || frame.opcode == 0x2) {
                    if (frame.fin) {
                        // Complete single-frame message.
                        state->_readCallback(std::span<const uint8_t>(frame.payload));
                    } else {
                        // First fragment — start accumulating.
                        state->_fragmentBuffer = std::move(frame.payload);
                    }
                } else if (frame.opcode == 0x0) {
                    // Continuation frame.
                    state->_fragmentBuffer.insert(
                        state->_fragmentBuffer.end(), frame.payload.begin(), frame.payload.end());
                    if (frame.fin) {
                        // Final fragment — deliver assembled message.
                        state->_readCallback(std::span<const uint8_t>(state->_fragmentBuffer));
                        state->_fragmentBuffer.clear();
                    }
                }
                // Reserved opcodes are silently ignored.
            }
        });
}

void WebSocketStream::readStop() noexcept
{
    _state->_readActive = false;
    _state->_socket.readStop();
    _state->_readCallback = {};
}

std::expected<void, scaler::wrapper::uv::Error> WebSocketStream::shutdown(
    scaler::wrapper::uv::ShutdownCallback callback) noexcept
{
    auto closeFrame = buildControlFrame(0x8, !_state->_isServer, {});
    auto frameData  = std::make_shared<std::vector<uint8_t>>(std::move(closeFrame));
    const std::span<const uint8_t> frameSpan(*frameData);
    auto state = _state;

    auto result = _state->_socket.write(
        std::span<const std::span<const uint8_t>>(&frameSpan, 1),
        [state, frameData = std::move(frameData), callback = std::move(callback)](
            std::expected<void, scaler::wrapper::uv::Error>) mutable {
            UV_EXIT_ON_ERROR(state->_socket.shutdown(std::move(callback)));
        });

    if (!result.has_value())
        return std::unexpected(result.error());
    return {};
}

std::expected<void, scaler::wrapper::uv::Error> WebSocketStream::closeReset() noexcept
{
    return _state->_socket.closeReset();
}

}  // namespace internal
}  // namespace ymq
}  // namespace scaler

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

static constexpr size_t MAX_UPGRADE_HEADER_SIZE = 64 * 1024;  // 64 KB

// WebSocket frame flags and masks (RFC 6455 section 5.2)
static constexpr uint8_t FLAG_FIN    = 0x80;
static constexpr uint8_t FLAG_MASKED = 0x80;
static constexpr uint8_t MASK_OPCODE = 0x0F;
static constexpr uint8_t MASK_LENGTH = 0x7F;

// WebSocket opcodes (RFC 6455 section 5.2)
static constexpr uint8_t OPCODE_CONTINUATION = 0x0;
static constexpr uint8_t OPCODE_TEXT         = 0x1;
static constexpr uint8_t OPCODE_BINARY       = 0x2;
static constexpr uint8_t OPCODE_CLOSE        = 0x8;
static constexpr uint8_t OPCODE_PING         = 0x9;
static constexpr uint8_t OPCODE_PONG         = 0xA;

// WebSocket payload length encoding (RFC 6455 section 5.2)
static constexpr uint8_t PAYLOAD_LEN_16BIT         = 126;
static constexpr uint8_t PAYLOAD_LEN_64BIT         = 127;
static constexpr size_t PAYLOAD_LEN_16BIT_MAX      = 65536;
static constexpr uint8_t MAX_CONTROL_FRAME_PAYLOAD = 125;

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

// Returns the frame header bytes for a server-side frame (unmasked).
std::vector<uint8_t> buildServerFrameHeader(size_t payloadSize) noexcept
{
    std::vector<uint8_t> header;
    header.push_back(FLAG_FIN | OPCODE_BINARY);
    if (payloadSize < PAYLOAD_LEN_16BIT) {
        header.push_back(static_cast<uint8_t>(payloadSize));
    } else if (payloadSize < PAYLOAD_LEN_16BIT_MAX) {
        header.push_back(PAYLOAD_LEN_16BIT);
        header.push_back(static_cast<uint8_t>((payloadSize >> 8) & 0xFF));
        header.push_back(static_cast<uint8_t>(payloadSize & 0xFF));
    } else {
        header.push_back(PAYLOAD_LEN_64BIT);
        for (int i = 7; i >= 0; --i)
            header.push_back(static_cast<uint8_t>((payloadSize >> (i * 8)) & 0xFF));
    }
    return header;
}

// Returns {header+mask, masked-payload-copy} for a client-side frame (masked, RFC 6455 section 5.3).
std::pair<std::vector<uint8_t>, std::vector<uint8_t>> buildClientFrame(
    std::span<const std::span<const uint8_t>> buffers, size_t totalSize) noexcept
{
    static thread_local std::mt19937 rng(std::random_device {}());
    std::uniform_int_distribution<uint32_t> dist;
    std::array<uint8_t, 4> maskKey;
    const uint32_t maskInt = dist(rng);
    std::memcpy(maskKey.data(), &maskInt, 4);

    std::vector<uint8_t> header;
    header.push_back(FLAG_FIN | OPCODE_BINARY);
    if (totalSize < PAYLOAD_LEN_16BIT) {
        header.push_back(FLAG_MASKED | static_cast<uint8_t>(totalSize));
    } else if (totalSize < PAYLOAD_LEN_16BIT_MAX) {
        header.push_back(FLAG_MASKED | PAYLOAD_LEN_16BIT);
        header.push_back(static_cast<uint8_t>((totalSize >> 8) & 0xFF));
        header.push_back(static_cast<uint8_t>(totalSize & 0xFF));
    } else {
        header.push_back(FLAG_MASKED | PAYLOAD_LEN_64BIT);
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

// Builds a control frame (CLOSE, PING, PONG). RFC 6455 section 5.5:
// control frames are always FIN=1 and carry at most 125 bytes of payload.
// Client frames must be masked; server frames must not.
std::vector<uint8_t> buildControlFrame(uint8_t opcode, bool isClient, std::span<const uint8_t> payload) noexcept
{
    static thread_local std::mt19937 rng(std::random_device {}());
    std::uniform_int_distribution<uint32_t> dist;

    const uint8_t len = static_cast<uint8_t>(payload.size());  // caller ensures <= 125

    std::vector<uint8_t> frame;
    frame.push_back(FLAG_FIN | opcode);
    if (isClient) {
        frame.push_back(FLAG_MASKED | len);
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

struct DecodedFrame {
    uint8_t opcode;
    bool fin;
    std::vector<uint8_t> payload;
};

// Tries to parse one complete WebSocket frame from buffer, consuming it in-place.
//   unexpected(error) - protocol error
//   {nullopt}         - buffer does not yet contain a full frame
//   {DecodedFrame}    - one frame decoded and consumed
std::expected<std::optional<DecodedFrame>, scaler::wrapper::uv::Error> tryDecodeFrame(
    std::vector<uint8_t>& buffer) noexcept
{
    if (buffer.size() < 2)
        return std::optional<DecodedFrame> {std::nullopt};

    const uint8_t byte0  = buffer[0];
    const uint8_t byte1  = buffer[1];
    const bool fin       = (byte0 & FLAG_FIN) != 0;
    const uint8_t opcode = byte0 & MASK_OPCODE;
    const bool masked    = (byte1 & FLAG_MASKED) != 0;
    uint64_t payloadLen  = byte1 & MASK_LENGTH;
    size_t headerSize    = 2;

    if (payloadLen == PAYLOAD_LEN_16BIT) {
        if (buffer.size() < 4)
            return std::optional<DecodedFrame> {std::nullopt};
        payloadLen = (uint64_t(buffer[2]) << 8) | buffer[3];
        headerSize = 4;
    } else if (payloadLen == PAYLOAD_LEN_64BIT) {
        if (buffer.size() < 10)
            return std::optional<DecodedFrame> {std::nullopt};
        payloadLen = 0;
        for (int i = 0; i < 8; ++i)
            payloadLen = (payloadLen << 8) | buffer[2 + i];
        headerSize = 10;
    }

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
void WebSocketStream::finishClientUpgrade(std::shared_ptr<ClientUpgradeContext> ctx) noexcept
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

    const auto headerMap = extractHeaders(headers);
    const auto acceptIt  = headerMap.find("sec-websocket-accept");
    if (acceptIt == headerMap.end() || acceptIt->second != computeWebSocketAccept(ctx->key)) {
        ctx->callback(std::unexpected(scaler::wrapper::uv::Error {UV_EPROTO}));
        return;
    }

    // Preserve any data that arrived after the HTTP headers (should be empty in practice).
    std::vector<uint8_t> leftover(
        ctx->recvBuffer.begin() + static_cast<std::ptrdiff_t>(headersEnd + 4), ctx->recvBuffer.end());

    ctx->callback(fromUpgradedSocket(std::move(ctx->socket.value()), false, std::move(leftover)));
}

// Called when a complete HTTP request has been assembled in the server upgrade context.
void WebSocketStream::finishServerUpgrade(std::shared_ptr<ServerUpgradeContext> ctx) noexcept
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

    const auto headerMap    = extractHeaders(headers);
    const auto upgradeIt    = headerMap.find("upgrade");
    const auto keyIt        = headerMap.find("sec-websocket-key");
    const auto connectionIt = headerMap.find("connection");
    const auto versionIt    = headerMap.find("sec-websocket-version");

    if (upgradeIt == headerMap.end() || keyIt == headerMap.end() || connectionIt == headerMap.end() ||
        versionIt == headerMap.end()) {
        ctx->callback(std::unexpected(scaler::wrapper::uv::Error {UV_EPROTO}));
        return;
    }

    // Verify Upgrade: websocket (case-insensitive)
    if (toLower(upgradeIt->second) != "websocket") {
        ctx->callback(std::unexpected(scaler::wrapper::uv::Error {UV_EPROTO}));
        return;
    }

    // Verify Connection header contains "upgrade" (case-insensitive, may be a token list)
    if (toLower(connectionIt->second).find("upgrade") == std::string::npos) {
        ctx->callback(std::unexpected(scaler::wrapper::uv::Error {UV_EPROTO}));
        return;
    }

    if (versionIt->second != "13") {
        ctx->callback(std::unexpected(scaler::wrapper::uv::Error {UV_EPROTO}));
        return;
    }

    const std::string response = buildServerUpgradeResponse(keyIt->second);
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
            ctx->callback(fromUpgradedSocket(std::move(ctx->socket.value()), true));
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

    auto requestData =
        std::make_shared<std::string>(buildClientUpgradeRequest(address.host, address.port, address.path, ctx->key));
    const std::span<const uint8_t> requestSpan(
        reinterpret_cast<const uint8_t*>(requestData->data()), requestData->size());

    auto writeResult = ctx->socket->write(
        requestSpan,
        [ctx, requestData = std::move(requestData)](std::expected<void, scaler::wrapper::uv::Error> result) mutable {
            if (!result.has_value()) {
                ctx->callback(std::unexpected(result.error()));
                return;
            }

            auto readStartResult = ctx->socket->readStart(
                [ctx](std::expected<std::span<const uint8_t>, scaler::wrapper::uv::Error> readResult) mutable {
                    if (!readResult.has_value()) {
                        auto safeCtx = ctx;
                        ctx->socket->readStop();
                        safeCtx->callback(std::unexpected(readResult.error()));
                        return;
                    }

                    const auto& data = readResult.value();
                    ctx->recvBuffer.insert(ctx->recvBuffer.end(), data.begin(), data.end());

                    if (ctx->recvBuffer.size() > MAX_UPGRADE_HEADER_SIZE) {
                        auto safeCtx = ctx;
                        ctx->socket->readStop();
                        safeCtx->callback(std::unexpected(scaler::wrapper::uv::Error {UV_EPROTO}));
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
                // Copy ctx to the stack before readStop() - readStop() destroys this lambda (and the
                // captured ctx) via setData({}), so ctx must outlive that call.
                auto safeCtx = ctx;
                ctx->socket->readStop();
                safeCtx->callback(std::unexpected(readResult.error()));
                return;
            }

            const auto& data = readResult.value();
            ctx->recvBuffer.insert(ctx->recvBuffer.end(), data.begin(), data.end());

            if (ctx->recvBuffer.size() > MAX_UPGRADE_HEADER_SIZE) {
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

void WebSocketStream::onRead(
    std::shared_ptr<State> state, std::expected<std::span<const uint8_t>, scaler::wrapper::uv::Error> result) noexcept
{
    if (!result.has_value()) {
        if (state->_readActive && state->_readCallback) {
            state->_readCallback(std::unexpected(result.error()));
        }
        return;
    }

    const auto& data = result.value();
    state->_recvBuffer.insert(state->_recvBuffer.end(), data.begin(), data.end());

    processRecvBuffer(state);
}

void WebSocketStream::processRecvBuffer(std::shared_ptr<State> state) noexcept
{
    while (state->_readActive && !state->_recvBuffer.empty()) {
        auto frameResult = tryDecodeFrame(state->_recvBuffer);
        if (!frameResult.has_value()) {
            state->_readCallback(std::unexpected(frameResult.error()));
            return;
        }
        if (!frameResult->has_value())
            break;  // need more data

        auto& frame = frameResult->value();

        if (frame.opcode == OPCODE_CLOSE) {
            // CLOSE: echo a CLOSE frame then signal clean disconnect.
            auto closeFrame = buildControlFrame(OPCODE_CLOSE, !state->_isServer, {});
            auto frameData  = std::make_shared<std::vector<uint8_t>>(std::move(closeFrame));
            const std::span<const uint8_t> frameSpan(*frameData);
            // Best-effort CLOSE echo - connection is shutting down regardless.
            if (auto r = state->_socket.write(
                    std::span<const std::span<const uint8_t>>(&frameSpan, 1),
                    [frameData = std::move(frameData)](std::expected<void, scaler::wrapper::uv::Error>) {});
                !r.has_value()) {
            }
            if (state->_readActive && state->_readCallback)
                state->_readCallback(std::unexpected(scaler::wrapper::uv::Error {UV_EOF}));
            return;
        }

        if (frame.opcode == OPCODE_PING) {
            // PING: respond with PONG carrying the same payload (RFC 6455 section 5.5.3).
            auto pongPayload = frame.payload;
            if (pongPayload.size() > MAX_CONTROL_FRAME_PAYLOAD)
                pongPayload.resize(MAX_CONTROL_FRAME_PAYLOAD);
            auto pongFrame = buildControlFrame(OPCODE_PONG, !state->_isServer, pongPayload);
            auto frameData = std::make_shared<std::vector<uint8_t>>(std::move(pongFrame));
            const std::span<const uint8_t> frameSpan(*frameData);
            // Best-effort PONG - if this write fails the next read will catch the error.
            if (auto r = state->_socket.write(
                    std::span<const std::span<const uint8_t>>(&frameSpan, 1),
                    [frameData = std::move(frameData)](std::expected<void, scaler::wrapper::uv::Error>) {});
                !r.has_value()) {
            }
            continue;
        }

        if (frame.opcode == OPCODE_PONG) {
            // PONG: unsolicited or in response to our PING - ignore.
            continue;
        }

        // Data frames: handle fragmentation per RFC 6455 section 5.4.
        if (frame.opcode == OPCODE_TEXT || frame.opcode == OPCODE_BINARY) {
            if (frame.fin) {
                // Complete single-frame message.
                state->_readCallback(std::span<const uint8_t>(frame.payload));
            } else {
                // First fragment - start accumulating.
                state->_fragmentBuffer = std::move(frame.payload);
            }
        } else if (frame.opcode == OPCODE_CONTINUATION) {
            // Continuation frame.
            state->_fragmentBuffer.insert(state->_fragmentBuffer.end(), frame.payload.begin(), frame.payload.end());
            if (frame.fin) {
                // Final fragment - deliver assembled message.
                state->_readCallback(std::span<const uint8_t>(state->_fragmentBuffer));
                state->_fragmentBuffer.clear();
            }
        }
        // Reserved opcodes are silently ignored.
    }
}

std::expected<void, scaler::wrapper::uv::Error> WebSocketStream::readStart(
    scaler::wrapper::uv::ReadCallback callback) noexcept
{
    auto state           = _state;
    state->_readCallback = std::move(callback);
    state->_readActive   = true;

    auto startResult = state->_socket.readStart(
        [state](std::expected<std::span<const uint8_t>, scaler::wrapper::uv::Error> result) mutable {
            onRead(state, std::move(result));
        });

    if (!startResult.has_value()) {
        return startResult;
    }

    // Drain any bytes already buffered from the WebSocket upgrade leftover.
    // Without this, if the peer sent WebSocket frames coalesced into the same TCP segment as the
    // HTTP upgrade response, those frames would sit in `_recvBuffer` until new data arrives.
    processRecvBuffer(_state);

    return startResult;
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
    auto closeFrame = buildControlFrame(OPCODE_CLOSE, !_state->_isServer, {});
    auto frameData  = std::make_shared<std::vector<uint8_t>>(std::move(closeFrame));
    const std::span<const uint8_t> frameSpan(*frameData);
    auto state = _state;

    // Share ownership so both the synchronous error path and the async write callback can invoke it.
    auto callbackPtr = std::make_shared<scaler::wrapper::uv::ShutdownCallback>(std::move(callback));

    auto result = _state->_socket.write(
        frameSpan,
        [state, frameData = std::move(frameData), callbackPtr](
            std::expected<void, scaler::wrapper::uv::Error> writeErr) mutable {
            if (!writeErr.has_value()) {
                // CLOSE frame failed (connection already gone) - treat as successful shutdown.
                (*callbackPtr)({});
                return;
            }
            // UV_ENOTCONN from the TCP shutdown callback means the peer already closed the
            // connection after we sent the CLOSE frame - treat as successful shutdown.
            auto r = state->_socket.shutdown(
                [callbackPtr](std::expected<void, scaler::wrapper::uv::Error> shutdownErr) mutable {
                    if (!shutdownErr.has_value() && shutdownErr.error().code() == UV_ENOTCONN)
                        (*callbackPtr)({});
                    else
                        (*callbackPtr)(shutdownErr);
                });
            if (!r.has_value()) {
                if (r.error().code() == UV_ENOTCONN)
                    (*callbackPtr)({});
                else
                    UV_EXIT_ON_ERROR(r);
            }
        });

    if (!result.has_value()) {
        if (result.error().code() == UV_ENOTCONN) {
            // Socket already disconnected - no CLOSE frame needed.
            (*callbackPtr)({});
            return {};
        }
        return std::unexpected(result.error());
    }
    return {};
}

WebSocketStream::~WebSocketStream() noexcept
{
    if (_state && _state->_readActive) {
        readStop();
    }
}

std::expected<void, scaler::wrapper::uv::Error> WebSocketStream::closeReset() noexcept
{
    readStop();
    return _state->_socket.closeReset();
}

}  // namespace internal
}  // namespace ymq
}  // namespace scaler

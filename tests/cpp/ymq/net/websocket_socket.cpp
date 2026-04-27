#include "tests/cpp/ymq/net/websocket_socket.h"

#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "scaler/ymq/address.h"
#include "scaler/ymq/internal/websocket_utils.h"

// ─── Shared WebSocketSocket implementation ────────────────────────────────────

WebSocketSocket::WebSocketSocket(long long fd, bool isServer): _fd(fd), _isServer(isServer)
{
}

std::unique_ptr<Socket> WebSocketSocket::accept() const
{
    const long long fd = rawAcceptFd();
    auto socket        = std::unique_ptr<WebSocketSocket>(new WebSocketSocket(fd, true));
    socket->performServerHandshake();
    return socket;
}

void WebSocketSocket::writeAll(const void* data, size_t size) const
{
    sendFrame(data, size);
}

void WebSocketSocket::writeAll(std::string msg) const
{
    writeAll(msg.data(), msg.size());
}

void WebSocketSocket::readExact(void* buffer, size_t size) const
{
    fillRecvBuffer(size);
    std::memcpy(buffer, _recvBuffer.data(), size);
    _recvBuffer.erase(_recvBuffer.begin(), _recvBuffer.begin() + static_cast<std::ptrdiff_t>(size));
}

void WebSocketSocket::writeMessage(std::string msg) const
{
    const uint64_t header = msg.length();
    writeAll(&header, sizeof(header));
    writeAll(msg.data(), msg.length());
}

std::string WebSocketSocket::readMessage() const
{
    uint64_t header = 0;
    readExact(&header, sizeof(header));
    std::vector<char> buf(header);
    readExact(buf.data(), header);
    return std::string(buf.data(), header);
}

void WebSocketSocket::rawWriteAll(const void* data, size_t size) const
{
    size_t cursor = 0;
    while (cursor < size)
        cursor += static_cast<size_t>(rawWrite(static_cast<const char*>(data) + cursor, size - cursor));
}

void WebSocketSocket::rawReadExact(void* buffer, size_t size) const
{
    size_t cursor = 0;
    while (cursor < size) {
        const int n = rawRead(static_cast<char*>(buffer) + cursor, size - cursor);
        if (n == 0)
            throw std::runtime_error("WebSocket: connection closed unexpectedly");
        cursor += static_cast<size_t>(n);
    }
}

void WebSocketSocket::sendFrame(const void* data, size_t size) const
{
    // Split large payloads into 1 MiB frames so the receiver can decode and deliver
    // each chunk without accumulating the entire payload before the first delivery.
    static constexpr size_t MAX_FRAME_PAYLOAD = 1024 * 1024;
    if (size > MAX_FRAME_PAYLOAD) {
        const auto* bytes = static_cast<const uint8_t*>(data);
        for (size_t offset = 0; offset < size; offset += MAX_FRAME_PAYLOAD) {
            sendFrame(bytes + offset, std::min(size - offset, MAX_FRAME_PAYLOAD));
        }
        return;
    }

    std::vector<uint8_t> header;
    header.push_back(0x82);  // FIN | binary opcode

    if (_isServer) {
        // Server sends unmasked frames (RFC 6455 §5.1)
        if (size < 126) {
            header.push_back(static_cast<uint8_t>(size));
        } else if (size < 65536) {
            header.push_back(126);
            header.push_back(static_cast<uint8_t>((size >> 8) & 0xFF));
            header.push_back(static_cast<uint8_t>(size & 0xFF));
        } else {
            header.push_back(127);
            for (int i = 7; i >= 0; --i)
                header.push_back(static_cast<uint8_t>((size >> (i * 8)) & 0xFF));
        }
        rawWriteAll(header.data(), header.size());
        rawWriteAll(data, size);
    } else {
        // Client sends masked frames (RFC 6455 §5.3)
        static thread_local std::mt19937 rng(std::random_device {}());
        std::uniform_int_distribution<uint32_t> dist;
        std::array<uint8_t, 4> maskKey;
        const uint32_t maskInt = dist(rng);
        std::memcpy(maskKey.data(), &maskInt, 4);

        if (size < 126) {
            header.push_back(0x80 | static_cast<uint8_t>(size));
        } else if (size < 65536) {
            header.push_back(0x80 | 126);
            header.push_back(static_cast<uint8_t>((size >> 8) & 0xFF));
            header.push_back(static_cast<uint8_t>(size & 0xFF));
        } else {
            header.push_back(0x80 | 127);
            for (int i = 7; i >= 0; --i)
                header.push_back(static_cast<uint8_t>((size >> (i * 8)) & 0xFF));
        }
        header.insert(header.end(), maskKey.begin(), maskKey.end());
        rawWriteAll(header.data(), header.size());

        std::vector<uint8_t> masked(size);
        const auto* bytes = static_cast<const uint8_t*>(data);
        for (size_t i = 0; i < size; ++i)
            masked[i] = bytes[i] ^ maskKey[i % 4];
        rawWriteAll(masked.data(), size);
    }
}

void WebSocketSocket::fillRecvBuffer(size_t needed) const
{
    while (_recvBuffer.size() < needed) {
        uint8_t header[2];
        rawReadExact(header, 2);

        const uint8_t opcode = header[0] & 0x0F;
        const bool masked    = (header[1] & 0x80) != 0;
        uint64_t payloadLen  = header[1] & 0x7F;

        if (payloadLen == 126) {
            uint8_t ext[2];
            rawReadExact(ext, 2);
            payloadLen = (uint64_t(ext[0]) << 8) | ext[1];
        } else if (payloadLen == 127) {
            uint8_t ext[8];
            rawReadExact(ext, 8);
            payloadLen = 0;
            for (int i = 0; i < 8; ++i)
                payloadLen = (payloadLen << 8) | ext[i];
        }

        std::array<uint8_t, 4> maskKey {};
        if (masked)
            rawReadExact(maskKey.data(), 4);

        std::vector<uint8_t> payload(static_cast<size_t>(payloadLen));
        rawReadExact(payload.data(), static_cast<size_t>(payloadLen));

        // Skip control frames (close=0x8, ping=0x9, pong=0xA) and reserved opcodes
        if (opcode != 0x0 && opcode != 0x1 && opcode != 0x2)
            continue;

        if (masked) {
            for (size_t i = 0; i < payload.size(); ++i)
                payload[i] ^= maskKey[i % 4];
        }

        _recvBuffer.insert(_recvBuffer.end(), payload.begin(), payload.end());
    }
}

void WebSocketSocket::performClientHandshake(const scaler::ymq::WebSocketAddress& address) const
{
    const std::string key     = scaler::ymq::internal::generateWebSocketKey();
    const std::string request = "GET " + address.path +
                                " HTTP/1.1\r\n"
                                "Host: " +
                                address.host + ":" + std::to_string(address.port) +
                                "\r\n"
                                "Upgrade: websocket\r\n"
                                "Connection: Upgrade\r\n"
                                "Sec-WebSocket-Key: " +
                                key +
                                "\r\n"
                                "Sec-WebSocket-Version: 13\r\n"
                                "\r\n";
    rawWriteAll(request.data(), request.size());

    std::string response;
    char ch;
    while (response.size() < 4 || response.compare(response.size() - 4, 4, "\r\n\r\n") != 0) {
        rawReadExact(&ch, 1);
        response += ch;
    }

    if (response.find("101") == std::string::npos)
        throw std::runtime_error("WebSocket handshake failed: server did not return 101");

    const auto accept = scaler::ymq::internal::extractHeader(
        std::string_view(response).substr(0, response.size() - 4), "Sec-WebSocket-Accept");
    if (!accept.has_value() || *accept != scaler::ymq::internal::computeWebSocketAccept(key))
        throw std::runtime_error("WebSocket handshake failed: invalid Sec-WebSocket-Accept");
}

void WebSocketSocket::performServerHandshake() const
{
    std::string request;
    char ch;
    while (request.size() < 4 || request.compare(request.size() - 4, 4, "\r\n\r\n") != 0) {
        rawReadExact(&ch, 1);
        request += ch;
    }

    const auto key = scaler::ymq::internal::extractHeader(
        std::string_view(request).substr(0, request.size() - 4), "Sec-WebSocket-Key");
    if (!key.has_value())
        throw std::runtime_error("WebSocket handshake failed: missing Sec-WebSocket-Key");

    const std::string response =
        "HTTP/1.1 101 Switching Protocols\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        "Sec-WebSocket-Accept: " +
        scaler::ymq::internal::computeWebSocketAccept(*key) +
        "\r\n"
        "\r\n";
    rawWriteAll(response.data(), response.size());
}

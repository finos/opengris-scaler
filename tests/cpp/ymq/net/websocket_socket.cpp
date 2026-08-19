#include "tests/cpp/ymq/net/websocket_socket.h"

#include <cstdint>
#include <cstring>
#include <map>
#include <memory>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "scaler/ymq/address.h"
#include "scaler/ymq/internal/websocket_utils.h"

// Split large payloads into 1 MiB frames so the receiver can decode and deliver
// each chunk without accumulating the entire payload before the first delivery.
static constexpr size_t maxFramePayload = 1024 * 1024;

// WebSocket frame flags and masks (RFC 6455 section 5.2)
static constexpr uint8_t flagFin    = 0x80;
static constexpr uint8_t flagMasked = 0x80;
static constexpr uint8_t maskOpcode = 0x0F;
static constexpr uint8_t maskLength = 0x7F;

// WebSocket opcodes (RFC 6455 section 5.2)
static constexpr uint8_t opcodeContinuation = 0x0;
static constexpr uint8_t opcodeText         = 0x1;
static constexpr uint8_t opcodeBinary       = 0x2;

// WebSocket payload length encoding (RFC 6455 section 5.2)
static constexpr uint8_t payloadLen16Bit   = 126;
static constexpr uint8_t payloadLen64Bit   = 127;
static constexpr size_t payloadLen16BitMax = 65536;

// WebSocket frame layout (RFC 6455 section 5.2)
static constexpr size_t baseHeaderSize    = 2;  // FIN/opcode byte + mask/length byte
static constexpr size_t extendedLen16Size = 2;
static constexpr size_t extendedLen64Size = 8;
static constexpr size_t maskKeySize       = 4;

static constexpr int bitsPerByte   = 8;
static constexpr uint64_t byteMask = 0xFF;

// A handshake request/response ends with a blank line.
static constexpr size_t headerTerminatorSize = 4;  // "\r\n\r\n"

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
    if (size > maxFramePayload) {
        const auto* bytes = static_cast<const uint8_t*>(data);
        for (size_t offset = 0; offset < size; offset += maxFramePayload) {
            sendFrame(bytes + offset, std::min(size - offset, maxFramePayload));
        }
        return;
    }

    std::vector<uint8_t> header;
    header.push_back(flagFin | opcodeBinary);

    if (_isServer) {
        // Server sends unmasked frames (RFC 6455 section 5.1)
        if (size < payloadLen16Bit) {
            header.push_back(static_cast<uint8_t>(size));
        } else if (size < payloadLen16BitMax) {
            header.push_back(payloadLen16Bit);
            header.push_back(static_cast<uint8_t>((size >> bitsPerByte) & byteMask));
            header.push_back(static_cast<uint8_t>(size & byteMask));
        } else {
            header.push_back(payloadLen64Bit);
            for (int i = static_cast<int>(extendedLen64Size) - 1; i >= 0; --i)
                header.push_back(static_cast<uint8_t>((size >> (i * bitsPerByte)) & byteMask));
        }
        rawWriteAll(header.data(), header.size());
        rawWriteAll(data, size);
    } else {
        // Client sends masked frames (RFC 6455 section 5.3)
        static thread_local std::mt19937 rng(std::random_device {}());
        std::uniform_int_distribution<uint32_t> dist;
        std::array<uint8_t, maskKeySize> maskKey;
        const uint32_t maskInt = dist(rng);
        std::memcpy(maskKey.data(), &maskInt, maskKeySize);

        if (size < payloadLen16Bit) {
            header.push_back(flagMasked | static_cast<uint8_t>(size));
        } else if (size < payloadLen16BitMax) {
            header.push_back(flagMasked | payloadLen16Bit);
            header.push_back(static_cast<uint8_t>((size >> bitsPerByte) & byteMask));
            header.push_back(static_cast<uint8_t>(size & byteMask));
        } else {
            header.push_back(flagMasked | payloadLen64Bit);
            for (int i = static_cast<int>(extendedLen64Size) - 1; i >= 0; --i)
                header.push_back(static_cast<uint8_t>((size >> (i * bitsPerByte)) & byteMask));
        }
        header.insert(header.end(), maskKey.begin(), maskKey.end());
        rawWriteAll(header.data(), header.size());

        std::vector<uint8_t> masked(size);
        const auto* bytes = static_cast<const uint8_t*>(data);
        for (size_t i = 0; i < size; ++i)
            masked[i] = bytes[i] ^ maskKey[i % maskKeySize];
        rawWriteAll(masked.data(), size);
    }
}

void WebSocketSocket::fillRecvBuffer(size_t needed) const
{
    while (_recvBuffer.size() < needed) {
        uint8_t header[baseHeaderSize];
        rawReadExact(header, baseHeaderSize);

        const uint8_t opcode = header[0] & maskOpcode;
        const bool masked    = (header[1] & flagMasked) != 0;
        uint64_t payloadLen  = header[1] & maskLength;

        if (payloadLen == payloadLen16Bit) {
            uint8_t ext[extendedLen16Size];
            rawReadExact(ext, extendedLen16Size);
            payloadLen = (uint64_t(ext[0]) << bitsPerByte) | ext[1];
        } else if (payloadLen == payloadLen64Bit) {
            uint8_t ext[extendedLen64Size];
            rawReadExact(ext, extendedLen64Size);
            payloadLen = 0;
            for (size_t i = 0; i < extendedLen64Size; ++i)
                payloadLen = (payloadLen << bitsPerByte) | ext[i];
        }

        std::array<uint8_t, maskKeySize> maskKey {};
        if (masked)
            rawReadExact(maskKey.data(), maskKeySize);

        std::vector<uint8_t> payload(static_cast<size_t>(payloadLen));
        rawReadExact(payload.data(), static_cast<size_t>(payloadLen));

        // Skip control frames (CLOSE, PING, PONG) and reserved opcodes
        if (opcode != opcodeContinuation && opcode != opcodeText && opcode != opcodeBinary)
            continue;

        if (masked) {
            for (size_t i = 0; i < payload.size(); ++i)
                payload[i] ^= maskKey[i % maskKeySize];
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
    while (response.size() < headerTerminatorSize ||
           response.compare(response.size() - headerTerminatorSize, headerTerminatorSize, "\r\n\r\n") != 0) {
        rawReadExact(&ch, 1);
        response += ch;
    }

    if (response.find("101") == std::string::npos)
        throw std::runtime_error("WebSocket handshake failed: server did not return 101");

    const auto headers = scaler::ymq::internal::extractHeaders(
        std::string_view(response).substr(0, response.size() - headerTerminatorSize));
    const auto acceptIt = headers.find("sec-websocket-accept");
    if (acceptIt == headers.end() || acceptIt->second != scaler::ymq::internal::computeWebSocketAccept(key))
        throw std::runtime_error("WebSocket handshake failed: invalid Sec-WebSocket-Accept");
}

void WebSocketSocket::performServerHandshake() const
{
    std::string request;
    char ch;
    while (request.size() < headerTerminatorSize ||
           request.compare(request.size() - headerTerminatorSize, headerTerminatorSize, "\r\n\r\n") != 0) {
        rawReadExact(&ch, 1);
        request += ch;
    }

    const auto reqHeaders = scaler::ymq::internal::extractHeaders(
        std::string_view(request).substr(0, request.size() - headerTerminatorSize));
    const auto keyIt = reqHeaders.find("sec-websocket-key");
    if (keyIt == reqHeaders.end())
        throw std::runtime_error("WebSocket handshake failed: missing Sec-WebSocket-Key");

    const std::string response =
        "HTTP/1.1 101 Switching Protocols\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        "Sec-WebSocket-Accept: " +
        scaler::ymq::internal::computeWebSocketAccept(keyIt->second) +
        "\r\n"
        "\r\n";
    rawWriteAll(response.data(), response.size());
}

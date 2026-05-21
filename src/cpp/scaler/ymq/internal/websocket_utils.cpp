#include "scaler/ymq/internal/websocket_utils.h"

#include <bit>
#include <cctype>
#include <cstdint>
#include <cstring>
#include <map>
#include <random>
#include <string>
#include <string_view>
#include <vector>

namespace scaler {
namespace ymq {
namespace internal {

std::array<uint8_t, 20> sha1(std::string_view input) noexcept
{
    uint32_t h[5] = {0x67452301, 0xEFCDAB89, 0x98BADCFE, 0x10325476, 0xC3D2E1F0};

    std::vector<uint8_t> msg(input.begin(), input.end());
    const uint64_t bitLen = msg.size() * 8;
    msg.push_back(0x80);
    while (msg.size() % 64 != 56)
        msg.push_back(0x00);
    for (int i = 7; i >= 0; --i)
        msg.push_back(static_cast<uint8_t>(bitLen >> (i * 8)));

    for (size_t offset = 0; offset < msg.size(); offset += 64) {
        uint32_t w[80];
        for (int i = 0; i < 16; ++i) {
            w[i] = (uint32_t(msg[offset + i * 4]) << 24) | (uint32_t(msg[offset + i * 4 + 1]) << 16) |
                   (uint32_t(msg[offset + i * 4 + 2]) << 8) | uint32_t(msg[offset + i * 4 + 3]);
        }
        for (int i = 16; i < 80; ++i)
            w[i] = std::rotl(w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16], 1);

        uint32_t a = h[0], b = h[1], c = h[2], d = h[3], e = h[4];
        for (int i = 0; i < 80; ++i) {
            uint32_t f, k;
            if (i < 20) {
                f = (b & c) | (~b & d);
                k = 0x5A827999;
            } else if (i < 40) {
                f = b ^ c ^ d;
                k = 0x6ED9EBA1;
            } else if (i < 60) {
                f = (b & c) | (b & d) | (c & d);
                k = 0x8F1BBCDC;
            } else {
                f = b ^ c ^ d;
                k = 0xCA62C1D6;
            }
            const uint32_t temp = std::rotl(a, 5) + f + e + k + w[i];
            e                   = d;
            d                   = c;
            c                   = std::rotl(b, 30);
            b                   = a;
            a                   = temp;
        }
        h[0] += a;
        h[1] += b;
        h[2] += c;
        h[3] += d;
        h[4] += e;
    }

    std::array<uint8_t, 20> digest;
    for (int i = 0; i < 5; ++i) {
        digest[i * 4]     = (h[i] >> 24) & 0xFF;
        digest[i * 4 + 1] = (h[i] >> 16) & 0xFF;
        digest[i * 4 + 2] = (h[i] >> 8) & 0xFF;
        digest[i * 4 + 3] = h[i] & 0xFF;
    }
    return digest;
}

std::string base64Encode(std::span<const uint8_t> data) noexcept
{
    static constexpr std::string_view kAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string result;
    result.reserve(((data.size() + 2) / 3) * 4);
    int val = 0, valb = -6;
    for (uint8_t c: data) {
        val = (val << 8) | c;
        valb += 8;
        while (valb >= 0) {
            result += kAlphabet[(val >> valb) & 0x3F];
            valb -= 6;
        }
    }
    if (valb > -6)
        result += kAlphabet[((val << 8) >> (valb + 8)) & 0x3F];
    while (result.size() % 4)
        result += '=';
    return result;
}

std::string toLower(std::string_view s) noexcept
{
    std::string result(s);
    for (char& c: result)
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return result;
}

std::string generateWebSocketKey() noexcept
{
    static thread_local std::mt19937 rng(std::random_device {}());
    std::uniform_int_distribution<uint32_t> dist;
    std::array<uint8_t, 16> keyBytes;
    for (size_t i = 0; i < keyBytes.size(); i += 4) {
        const uint32_t v = dist(rng);
        std::memcpy(&keyBytes[i], &v, 4);
    }
    return base64Encode(std::span<const uint8_t>(keyBytes));
}

std::string computeWebSocketAccept(const std::string& key) noexcept
{
    const std::string_view magic = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    const std::string input      = key + std::string(magic);
    const auto digest            = sha1(input);
    return base64Encode(std::span<const uint8_t>(digest));
}

std::map<std::string, std::string> extractHeaders(std::string_view headers) noexcept
{
    std::map<std::string, std::string> result;
    // Skip the request/status line.
    size_t pos = headers.find("\r\n");
    if (pos == std::string_view::npos)
        return result;
    pos += 2;
    while (pos < headers.size()) {
        const size_t lineEnd        = headers.find("\r\n", pos);
        const size_t end            = (lineEnd == std::string_view::npos) ? headers.size() : lineEnd;
        const std::string_view line = headers.substr(pos, end - pos);
        const size_t colon          = line.find(':');
        if (colon != std::string_view::npos) {
            std::string name  = toLower(line.substr(0, colon));
            size_t valueStart = colon + 1;
            while (valueStart < line.size() && (line[valueStart] == ' ' || line[valueStart] == '\t'))
                ++valueStart;
            result[std::move(name)] = std::string(line.substr(valueStart));
        }
        if (lineEnd == std::string_view::npos)
            break;
        pos = lineEnd + 2;
    }
    return result;
}

}  // namespace internal
}  // namespace ymq
}  // namespace scaler

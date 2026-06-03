#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <map>
#include <span>
#include <string>

#include "scaler/ymq/internal/websocket_utils.h"

class WebSocketUtilsTest: public ::testing::Test {};

// ---------------------------------------------------------------------------
// sha1
// ---------------------------------------------------------------------------

TEST_F(WebSocketUtilsTest, SHA1EmptyString)
{
    // FIPS 180-4 test vector: SHA-1("") = da39a3ee5e6b4b0d3255bfef95601890afd80709
    const std::array<uint8_t, 20> expected = {
        0xda, 0x39, 0xa3, 0xee, 0x5e, 0x6b, 0x4b, 0x0d, 0x32, 0x55,
        0xbf, 0xef, 0x95, 0x60, 0x18, 0x90, 0xaf, 0xd8, 0x07, 0x09,
    };
    EXPECT_EQ(scaler::ymq::internal::sha1(""), expected);
}

TEST_F(WebSocketUtilsTest, SHA1ShortString)
{
    // SHA-1("abc") = a9993e364706816aba3e25717850c26c9cd0d89d
    const std::array<uint8_t, 20> expected = {
        0xa9, 0x99, 0x3e, 0x36, 0x47, 0x06, 0x81, 0x6a, 0xba, 0x3e,
        0x25, 0x71, 0x78, 0x50, 0xc2, 0x6c, 0x9c, 0xd0, 0xd8, 0x9d,
    };
    EXPECT_EQ(scaler::ymq::internal::sha1("abc"), expected);
}

TEST_F(WebSocketUtilsTest, SHA1LongerString)
{
    // SHA-1("The quick brown fox jumps over the lazy dog")
    //   = 2fd4e1c67a2d28fced849ee1bb76e7391b93eb12
    const std::array<uint8_t, 20> expected = {
        0x2f, 0xd4, 0xe1, 0xc6, 0x7a, 0x2d, 0x28, 0xfc, 0xed, 0x84,
        0x9e, 0xe1, 0xbb, 0x76, 0xe7, 0x39, 0x1b, 0x93, 0xeb, 0x12,
    };
    EXPECT_EQ(scaler::ymq::internal::sha1("The quick brown fox jumps over the lazy dog"), expected);
}

// Input long enough to span two 64-byte SHA-1 blocks.
TEST_F(WebSocketUtilsTest, SHA1MultiBlock)
{
    // SHA-1("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq")
    //   = 84983e441c3bd26ebaae4aa1f95129e5e54670f1
    const std::array<uint8_t, 20> expected = {
        0x84, 0x98, 0x3e, 0x44, 0x1c, 0x3b, 0xd2, 0x6e, 0xba, 0xae,
        0x4a, 0xa1, 0xf9, 0x51, 0x29, 0xe5, 0xe5, 0x46, 0x70, 0xf1,
    };
    EXPECT_EQ(scaler::ymq::internal::sha1("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"), expected);
}

// ---------------------------------------------------------------------------
// base64Encode
// ---------------------------------------------------------------------------

static std::string encodeStr(std::string_view s)
{
    const auto* ptr = reinterpret_cast<const uint8_t*>(s.data());
    return scaler::ymq::internal::base64Encode(std::span<const uint8_t>(ptr, s.size()));
}

TEST_F(WebSocketUtilsTest, Base64EncodeEmpty)
{
    EXPECT_EQ(encodeStr(""), "");
}

TEST_F(WebSocketUtilsTest, Base64EncodePadTwo)
{
    // "citi" -> 4 bytes (4 mod 3 = 1) -> two padding chars
    EXPECT_EQ(encodeStr("citi"), "Y2l0aQ==");
}

TEST_F(WebSocketUtilsTest, Base64EncodePadOne)
{
    // "citibank" -> 8 bytes (8 mod 3 = 2) -> one padding char
    EXPECT_EQ(encodeStr("citibank"), "Y2l0aWJhbms=");
}

TEST_F(WebSocketUtilsTest, Base64EncodeNoPad)
{
    // "scaler" -> 6 bytes (multiple of 3) -> no padding
    EXPECT_EQ(encodeStr("scaler"), "c2NhbGVy");
}

TEST_F(WebSocketUtilsTest, Base64EncodeMultipleGroups)
{
    // "opengris" -> 8 bytes across multiple encoding groups
    EXPECT_EQ(encodeStr("opengris"), "b3BlbmdyaXM=");
}

// ---------------------------------------------------------------------------
// computeWebSocketAccept
// ---------------------------------------------------------------------------

TEST_F(WebSocketUtilsTest, ComputeWebSocketAcceptRFC6455Example)
{
    // RFC 6455 section 1.3 example: key "dGhlIHNhbXBsZSBub25jZQ==" -> "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
    EXPECT_EQ(
        scaler::ymq::internal::computeWebSocketAccept("dGhlIHNhbXBsZSBub25jZQ=="), "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=");
}

TEST_F(WebSocketUtilsTest, ExtractHeadersBasic)
{
    const std::string headers =
        "GET / HTTP/1.1\r\n"
        "Host: 127.0.0.1\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
        "Sec-WebSocket-Version: 13\r\n";

    const auto map = scaler::ymq::internal::extractHeaders(headers);
    EXPECT_EQ(map.at("host"), "127.0.0.1");
    EXPECT_EQ(map.at("upgrade"), "websocket");
    EXPECT_EQ(map.at("connection"), "Upgrade");
    EXPECT_EQ(map.at("sec-websocket-key"), "dGhlIHNhbXBsZSBub25jZQ==");
    EXPECT_EQ(map.at("sec-websocket-version"), "13");
}

TEST_F(WebSocketUtilsTest, ExtractHeadersKeysCaseInsensitive)
{
    const std::string headers =
        "HTTP/1.1 101 Switching Protocols\r\n"
        "UPGRADE: WEBSOCKET\r\n"
        "Sec-WebSocket-Accept: abc123\r\n";

    const auto map = scaler::ymq::internal::extractHeaders(headers);
    EXPECT_TRUE(map.count("upgrade"));
    EXPECT_TRUE(map.count("sec-websocket-accept"));
}

TEST_F(WebSocketUtilsTest, ExtractHeadersValuesPreserveCase)
{
    const std::string headers =
        "GET / HTTP/1.1\r\n"
        "Sec-WebSocket-Accept: AbCdEfGh==\r\n";

    const auto map = scaler::ymq::internal::extractHeaders(headers);
    EXPECT_EQ(map.at("sec-websocket-accept"), "AbCdEfGh==");
}

TEST_F(WebSocketUtilsTest, ExtractHeadersNoSpaceAfterColon)
{
    const std::string headers =
        "GET / HTTP/1.1\r\n"
        "Upgrade:websocket\r\n"
        "Connection:Upgrade\r\n";

    const auto map = scaler::ymq::internal::extractHeaders(headers);
    EXPECT_EQ(map.at("upgrade"), "websocket");
    EXPECT_EQ(map.at("connection"), "Upgrade");
}

TEST_F(WebSocketUtilsTest, ExtractHeadersEmptyOrRequestLineOnly)
{
    EXPECT_TRUE(scaler::ymq::internal::extractHeaders("").empty());
    EXPECT_TRUE(scaler::ymq::internal::extractHeaders("GET / HTTP/1.1").empty());
    EXPECT_TRUE(scaler::ymq::internal::extractHeaders("GET / HTTP/1.1\r\n").empty());
}

TEST_F(WebSocketUtilsTest, ExtractHeadersMissingHeaderReturnsEnd)
{
    const std::string headers =
        "GET / HTTP/1.1\r\n"
        "Upgrade: websocket\r\n";

    const auto map = scaler::ymq::internal::extractHeaders(headers);
    EXPECT_EQ(map.find("sec-websocket-key"), map.end());
}

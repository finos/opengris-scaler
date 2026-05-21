#pragma once

#include <array>
#include <cstdint>
#include <map>
#include <span>
#include <string>
#include <string_view>

namespace scaler {
namespace ymq {
namespace internal {

std::array<uint8_t, 20> sha1(std::string_view input) noexcept;
std::string base64Encode(std::span<const uint8_t> data) noexcept;
std::string toLower(std::string_view s) noexcept;
std::string generateWebSocketKey() noexcept;
std::string computeWebSocketAccept(const std::string& key) noexcept;

// Parses all headers from an HTTP request/response block (including the first request/status line)
// in a single pass. Returns a map with lowercase header names and original-case values.
// Handles both "Name: value" and "Name:value" (RFC 7230 optional whitespace).
std::map<std::string, std::string> extractHeaders(std::string_view headers) noexcept;

}  // namespace internal
}  // namespace ymq
}  // namespace scaler

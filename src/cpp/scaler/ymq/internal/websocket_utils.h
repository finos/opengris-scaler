#pragma once

#include <array>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>

namespace scaler {
namespace ymq {
namespace internal {

std::array<uint8_t, 20> sha1(std::string_view input) noexcept;
std::string base64Encode(std::span<const uint8_t> data) noexcept;
std::string generateWebSocketKey() noexcept;
std::string computeWebSocketAccept(const std::string& key) noexcept;

// Case-insensitive header value extraction. Handles both "Name: value" and "Name:value"
// (RFC 7230 optional whitespace).
std::optional<std::string> extractHeader(std::string_view headers, std::string_view name) noexcept;

}  // namespace internal
}  // namespace ymq
}  // namespace scaler

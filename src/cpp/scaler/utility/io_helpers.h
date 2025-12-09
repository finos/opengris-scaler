#pragma once

#include <cstdint>
#include <functional>
#include <span>

#include "scaler/utility/io_result.h"

namespace scaler {
namespace utility {

// Attempt to read data into the specified buffer using the provided reader function.
//
// It continues reading until the buffer is completely filled or an error occurs.
IOResult readExact(std::span<uint8_t> buffer, std::function<IOResult(const std::span<uint8_t>&)> reader) noexcept;

// Writes all data from a sequence of byte spans using a provided writer function.
//
// It continues writing until all data is successfully written or an error occurs.
IOResult writeAll(
    const std::vector<std::span<const uint8_t>>& buffers,
    std::function<IOResult(const std::vector<std::span<const uint8_t>>&)> writer) noexcept;

}  // namespace utility
}  // namespace scaler

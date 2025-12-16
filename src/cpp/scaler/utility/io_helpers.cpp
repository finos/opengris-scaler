#include "scaler/utility/io_helpers.h"

#include <cassert>
#include <cstdint>
#include <functional>
#include <list>
#include <span>

namespace scaler {
namespace utility {

IOResult readExact(std::span<uint8_t> buffer, std::function<IOResult(std::span<uint8_t>)> reader) noexcept
{
    size_t cursor = 0;

    while (cursor < buffer.size()) {
        IOResult result = reader(buffer.subspan(cursor));
        cursor += result.bytesTransferred;

        if (result.error) {
            return IOResult::failure(result.error.value(), cursor);
        }
    }

    return IOResult::success(cursor);
}

IOResult writeAll(
    const std::vector<std::span<const uint8_t>>& buffers,
    std::function<IOResult(const std::vector<std::span<const uint8_t>>&)> writer) noexcept
{
    if (buffers.empty()) {
        return IOResult::success(0);
    }

    // Copies the buffers to a linked list, so that we can pop() the first items in O(1).
    std::list<std::span<const uint8_t>> bufferList(buffers.cbegin(), buffers.cend());

    size_t cursor = 0;

    while (!bufferList.empty()) {
        // O(n_buffers) ... could be O(1) with std::ranges but the signature becomes messy
        std::vector<std::span<const uint8_t>> currentBuffers(bufferList.cbegin(), bufferList.cend());

        IOResult result = writer(currentBuffers);
        cursor += result.bytesTransferred;

        if (result.error) {
            return IOResult::failure(result.error.value(), cursor);
        }

        // Update the buffer list, removing the already written buffers.
        // Worst time complexity is O(n_buffers)

        size_t bytesTransferred = result.bytesTransferred;

        while (bytesTransferred > 0) {
            assert(!bufferList.empty());
            std::span<const uint8_t>& frontBuffer = bufferList.front();

            if (bytesTransferred >= frontBuffer.size()) {
                bytesTransferred -= frontBuffer.size();
                bufferList.pop_front();
            } else {
                frontBuffer      = frontBuffer.subspan(bytesTransferred);
                bytesTransferred = 0;
            }
        }
    }

    return IOResult::success(cursor);
}

}  // namespace utility
}  // namespace scaler

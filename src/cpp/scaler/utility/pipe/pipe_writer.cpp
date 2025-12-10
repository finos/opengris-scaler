#include "scaler/utility/pipe/pipe_writer.h"

#include <cstdint>
#include <span>

#include "scaler/utility/io_helpers.h"
#include "scaler/utility/io_result.h"

namespace scaler {
namespace utility {
namespace pipe {

IOResult PipeWriter::writeAll(const std::vector<std::span<const uint8_t>>& buffers) const noexcept
{
    return utility::writeAll(
        buffers, [this](const std::vector<std::span<const uint8_t>>& buffers) { return this->writeBytes(buffers); });
}

}  // namespace pipe
}  // namespace utility
}  // namespace scaler

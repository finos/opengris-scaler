#include "scaler/utility/pipe/pipe_writer.h"

#include <cstdint>
#include <span>

#include "scaler/utility/io_result.h"

namespace scaler {
namespace utility {
namespace pipe {

IOResult PipeWriter::writeAll(std::span<const uint8_t> buffer) const noexcept
{
    size_t cursor = 0;

    while (cursor < buffer.size()) {
        IOResult result = this->write(buffer.subspan(cursor));
        cursor += result.bytesTransferred;

        if (result.error) {
            return IOResult::failure(result.error.value(), cursor);
        }
    }

    return IOResult::success(cursor);
}

}  // namespace pipe
}  // namespace utility
}  // namespace scaler

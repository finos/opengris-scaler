#include "scaler/utility/pipe/pipe_reader.h"

#include <cstdint>
#include <span>

#include "scaler/utility/io_helpers.h"
#include "scaler/utility/io_result.h"

namespace scaler {
namespace utility {
namespace pipe {

IOResult PipeReader::readExact(std::span<uint8_t> buffer) const noexcept
{
    return utility::readExact(buffer, [this](std::span<uint8_t> buffer) { return this->readBytes(buffer); });
}

}  // namespace pipe
}  // namespace utility
}  // namespace scaler

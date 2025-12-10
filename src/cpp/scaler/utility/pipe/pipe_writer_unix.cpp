#include <unistd.h>

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <span>

#include "scaler/utility/error.h"
#include "scaler/utility/io_result.h"
#include "scaler/utility/pipe/pipe_writer.h"

namespace scaler {
namespace utility {
namespace pipe {

PipeWriter::~PipeWriter()
{
    if (this->_fd == -1) {
        return;
    }

    close(this->_fd);
}

IOResult PipeWriter::writeBytes(const std::vector<std::span<const uint8_t>>& buffers) const noexcept
{
    size_t bytesTransferred = 0;

    for (const auto& buffer: buffers) {  // TODO: use sendmsg() instead
        ssize_t n;
        do {
            n = ::write(this->_fd, buffer.data(), buffer.size());
        } while (n < 0 && errno == EINTR);

        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return IOResult::failure(IOResult::Error::WouldBlock, bytesTransferred);
            } else {
                unrecoverableError({
                    Error::ErrorCode::CoreBug,
                    "Originated from",
                    "write(2)",
                    "Errno is",
                    strerror(errno),
                });
            }
        }

        bytesTransferred += n;
    }

    return IOResult::success(bytesTransferred);
}

}  // namespace pipe
}  // namespace utility
}  // namespace scaler

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

IOResult PipeWriter::write(std::span<const uint8_t> buffer) const noexcept
{
    ssize_t n;
    do {
        n = ::write(this->_fd, buffer.data(), buffer.size());
    } while (n < 0 && errno == EINTR);

    if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return IOResult::failure(IOResult::Error::WouldBlock);
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
    return IOResult::success(n);
}

}  // namespace pipe
}  // namespace utility
}  // namespace scaler

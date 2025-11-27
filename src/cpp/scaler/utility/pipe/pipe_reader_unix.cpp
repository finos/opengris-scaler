#include <unistd.h>

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <span>

#include "scaler/utility/error.h"
#include "scaler/utility/io_result.h"
#include "scaler/utility/pipe/pipe_reader.h"

namespace scaler {
namespace utility {
namespace pipe {

PipeReader::~PipeReader()
{
    if (this->_fd == -1) {
        return;
    }

    close(this->_fd);
}

IOResult PipeReader::read(std::span<uint8_t> buffer) const noexcept
{
    ssize_t n;
    do {
        n = ::read(this->_fd, buffer.data(), buffer.size());
    } while (n < 0 && errno == EINTR);

    if (n == 0) {
        return IOResult::failure(IOResult::Error::EndOfFile);
    }

    if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return IOResult::failure(IOResult::Error::WouldBlock);
        } else {
            unrecoverableError({
                Error::ErrorCode::CoreBug,
                "Originated from",
                "read(2)",
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

#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include "scaler/utility/io_result.h"
#include "scaler/utility/pipe/pipe_utils.h"

namespace scaler {
namespace utility {
namespace pipe {

class PipeWriter {
public:
    PipeWriter(int64_t fd): _fd(fd) {}
    ~PipeWriter();

    PipeWriter(PipeWriter&& other) noexcept
    {
        this->_fd = other._fd;
        other._fd = -1;
    }

    PipeWriter& operator=(PipeWriter&& other) noexcept
    {
        this->_fd = other._fd;
        other._fd = -1;
        return *this;
    }

    // Move-only
    PipeWriter(const PipeWriter&)            = delete;
    PipeWriter& operator=(const PipeWriter&) = delete;

    // Try to write all buffers, stopping on the first error. Return the total number of bytes written.
    IOResult writeAll(const std::vector<std::span<const uint8_t>>& buffers) const noexcept;

    // returns the native handle for this pipe writer
    // on linux, this is a pointer to the file descriptor
    // on windows, this is the HANDLE
    const int64_t fd() const noexcept { return _fd; }

    void setNonBlocking() const noexcept { pipe::setNonBlocking(_fd); }

private:
    // the native handle for this pipe reader
    // on Linux, this is a file descriptor
    // on Windows, this is a HANDLE
    int64_t _fd;

    IOResult writeBytes(const std::vector<std::span<const uint8_t>>& buffers) const noexcept;
};

}  // namespace pipe
}  // namespace utility
}  // namespace scaler

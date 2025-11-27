#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

#include "scaler/utility/io_result.h"
#include "scaler/utility/pipe/pipe_utils.h"

namespace scaler {
namespace utility {
namespace pipe {

class PipeReader {
public:
    PipeReader(int64_t fd): _fd(fd) {}
    ~PipeReader();

    PipeReader(PipeReader&& other) noexcept
    {
        this->_fd = other._fd;
        other._fd = -1;
    }

    PipeReader& operator=(PipeReader&& other) noexcept
    {
        this->_fd = other._fd;
        other._fd = -1;
        return *this;
    }

    // Move-only
    PipeReader(const PipeReader&)            = delete;
    PipeReader& operator=(const PipeReader&) = delete;

    // read up to buffer.size(), returning the number of bytes read.
    IOResult read(std::span<uint8_t> buffer) const noexcept;

    // read exactly buffer.size().
    IOResult readExact(std::span<uint8_t> buffer) const noexcept;

    // returns the native handle for this pipe reader
    // on linux, this is a pointer to the file descriptor
    // on windows, this is the HANDLE
    const int64_t fd() const noexcept { return _fd; }

    void setNonBlocking() const noexcept { pipe::setNonBlocking(_fd); }

private:
    // the native handle for this pipe reader
    // on Linux, this is a file descriptor
    // on Windows, this is a HANDLE
    int64_t _fd;
};

}  // namespace pipe
}  // namespace utility
}  // namespace scaler

#pragma once

#include <cstddef>

namespace scaler {
namespace utility {
namespace pipe {

class PipeWriter {
public:
    PipeWriter(long long fd);
    ~PipeWriter();

    // Move-only
    PipeWriter(PipeWriter&&) noexcept;
    PipeWriter& operator=(PipeWriter&&) noexcept;
    PipeWriter(const PipeWriter&)            = delete;
    PipeWriter& operator=(const PipeWriter&) = delete;

    // write `size` bytes
    void write_all(const void* data, size_t size) noexcept;

    // returns the native handle for this pipe writer
    // on linux, this is a pointer to the file descriptor
    // on windows, this is the HANDLE
    const long long fd() const noexcept;

private:
    // the native handle for this pipe reader
    // on Linux, this is a file descriptor
    // on Windows, this is a HANDLE
    long long _fd;

    // write up to `size` bytes
    int write(const void* buffer, size_t size) noexcept;
};

}  // namespace pipe
}  // namespace utility
}  // namespace scaler

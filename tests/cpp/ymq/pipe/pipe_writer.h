#pragma once

#include <cstddef>

struct Pipe;

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
    void writeAll(const void* data, size_t size);

private:
    // the native handle for this pipe reader
    // on Linux, this is a file descriptor
    // on Windows, this is a HANDLE
    long long _fd;

    // write up to `size` bytes
    int write(const void* buffer, size_t size);
};

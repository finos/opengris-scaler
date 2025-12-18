#pragma once

#include <cstddef>

struct Pipe;

class PipeReader {
public:
    PipeReader(long long fd);
    ~PipeReader();

    // Move-only
    PipeReader(PipeReader&&) noexcept;
    PipeReader& operator=(PipeReader&&) noexcept;
    PipeReader(const PipeReader&)            = delete;
    PipeReader& operator=(const PipeReader&) = delete;

    // read exactly `size` bytes
    void readExact(void* buffer, size_t size) const;

    // returns the native handle for this pipe reader
    // on linux, this is a pointer to the file descriptor
    // on windows, this is the HANDLE
    const long long fd() const noexcept;

private:
    // the native handle for this pipe reader
    // on Linux, this is a file descriptor
    // on Windows, this is a HANDLE
    long long _fd;

    // read up to `size` bytes
    int read(void* buffer, size_t size) const;
};

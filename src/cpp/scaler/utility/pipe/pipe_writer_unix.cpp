#include <unistd.h>

#include <cerrno>
#include <cstddef>
#include <cstdint>

#include "scaler/utility/error.h"
#include "scaler/utility/pipe/pipe_writer.h"

namespace scaler {
namespace utility {
namespace pipe {

PipeWriter::PipeWriter(int64_t fd): _fd(fd)
{
}

PipeWriter::~PipeWriter()
{
    close(this->_fd);
}

PipeWriter::PipeWriter(PipeWriter&& other) noexcept
{
    this->_fd = other._fd;
    other._fd = -1;
}

PipeWriter& PipeWriter::operator=(PipeWriter&& other) noexcept
{
    this->_fd = other._fd;
    other._fd = -1;
    return *this;
}

const int64_t PipeWriter::fd() const noexcept
{
    return this->_fd;
}

int PipeWriter::write(const void* buffer, size_t size) noexcept
{
    ssize_t n = ::write(this->_fd, buffer, size);
    if (n < 0) {
        unrecoverableError({
            Error::ErrorCode::CoreBug,
            "Originated from",
            "write(2)",
            "Errno is",
            strerror(errno),
        });
    }
    return n;
}

void PipeWriter::write_all(const void* buffer, size_t size) noexcept
{
    size_t cursor = 0;
    while (cursor < size)
        cursor += (size_t)this->write((char*)buffer + cursor, size - cursor);
}

}  // namespace pipe
}  // namespace utility
}  // namespace scaler

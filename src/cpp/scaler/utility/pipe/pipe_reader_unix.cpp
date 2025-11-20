#include <unistd.h>

#include <cerrno>
#include <cstddef>
#include <cstdint>

#include "scaler/utility/error.h"
#include "scaler/utility/pipe/pipe_reader.h"

namespace scaler {
namespace utility {
namespace pipe {

PipeReader::PipeReader(int64_t fd): _fd(fd)
{
}

PipeReader::~PipeReader()
{
    close(this->_fd);
}

PipeReader::PipeReader(PipeReader&& other) noexcept
{
    this->_fd = other._fd;
    other._fd = -1;
}

PipeReader& PipeReader::operator=(PipeReader&& other) noexcept
{
    this->_fd = other._fd;
    other._fd = -1;
    return *this;
}

const int64_t PipeReader::fd() const noexcept
{
    return this->_fd;
}

int PipeReader::read(void* buffer, size_t size) const noexcept
{
    ssize_t n = ::read(this->_fd, buffer, size);
    if (n < 0) {
        unrecoverableError({
            Error::ErrorCode::CoreBug,
            "Originated from",
            "read(2)",
            "Errno is",
            strerror(errno),
        });
    }
    return n;
}

void PipeReader::read_exact(void* buffer, size_t size) const noexcept
{
    size_t cursor = 0;
    while (cursor < size)
        cursor += (size_t)this->read((char*)buffer + cursor, size - cursor);
}

}  // namespace pipe
}  // namespace utility
}  // namespace scaler

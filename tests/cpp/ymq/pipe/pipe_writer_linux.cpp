#include <unistd.h>

#include <cstddef>

#include "tests/cpp/ymq/common/utils.h"
#include "tests/cpp/ymq/pipe/pipe_writer.h"

PipeWriter::PipeWriter(long long fd): _fd(fd)
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

int PipeWriter::write(const void* buffer, size_t size)
{
    ssize_t n = ::write(this->_fd, buffer, size);
    if (n < 0)
        raiseSystemError("write");
    return n;
}

void PipeWriter::writeAll(const void* buffer, size_t size)
{
    size_t cursor = 0;
    while (cursor < size)
        cursor += (size_t)this->write((char*)buffer + cursor, size - cursor);
}

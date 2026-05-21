#include <Windows.h>

#include <cstddef>

#include "tests/cpp/ymq/common/utils.h"
#include "tests/cpp/ymq/pipe/pipe_writer.h"

PipeWriter::PipeWriter(long long fd): _fd(fd)
{
}

PipeWriter::~PipeWriter()
{
    CloseHandle((HANDLE)this->_fd);
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
    DWORD bytes_written = 0;
    if (!WriteFile((HANDLE)this->_fd, buffer, (DWORD)size, &bytes_written, nullptr))
        raiseSystemError("failed to write to pipe");
    return bytes_written;
}

void PipeWriter::writeAll(const void* buffer, size_t size)
{
    size_t cursor = 0;
    while (cursor < size)
        cursor += (size_t)this->write((char*)buffer + cursor, size - cursor);
}

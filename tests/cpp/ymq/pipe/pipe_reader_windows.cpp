#include <Windows.h>

#include <cstddef>

#include "tests/cpp/ymq/common/utils.h"
#include "tests/cpp/ymq/pipe/pipe_reader.h"

PipeReader::PipeReader(long long fd): _fd(fd)
{
}

PipeReader::~PipeReader()
{
    CloseHandle((HANDLE)this->_fd);
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

const long long PipeReader::fd() const noexcept
{
    return this->_fd;
}

int PipeReader::read(void* buffer, size_t size) const
{
    DWORD bytes_read = 0;
    if (!ReadFile((HANDLE)this->_fd, buffer, (DWORD)size, &bytes_read, nullptr))
        raise_system_error("failed to read");
    return bytes_read;
}

void PipeReader::readExact(void* buffer, size_t size) const
{
    size_t cursor = 0;
    while (cursor < size)
        cursor += (size_t)this->read((char*)buffer + cursor, size - cursor);
}

#include <Windows.h>

#include <cstddef>
#include <cstdint>

#include "scaler/utility/pipe/pipe_writer.h"

namespace scaler {
namespace utility {
namespace pipe {

PipeWriter::PipeWriter(int64_t fd): _fd(fd)
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

const int64_t PipeReader::fd() const noexcept
{
    return this->_fd;
}

int PipeWriter::write(const void* buffer, size_t size) noexcept
{
    DWORD bytes_written = 0;
    if (!WriteFile((HANDLE)this->_fd, buffer, (DWORD)size, &bytes_written, nullptr)) {
        unrecoverableError({
            Error::ErrorCode::CoreBug,
            "Originated from",
            "WriteFile()",
        });
    }
    return bytes_written;
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

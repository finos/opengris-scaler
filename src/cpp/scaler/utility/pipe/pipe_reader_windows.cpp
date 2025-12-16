#include <Windows.h>

#include <cstddef>
#include <cstdint>
#include <span>

#include "scaler/utility/error.h"
#include "scaler/utility/io_result.h"
#include "scaler/utility/pipe/pipe_reader.h"

namespace scaler {
namespace utility {
namespace pipe {

PipeReader::~PipeReader()
{
    if (this->_fd == -1) {
        return;
    }

    CloseHandle((HANDLE)this->_fd);
}

IOResult PipeReader::readBytes(std::span<uint8_t> buffer) const noexcept
{
    DWORD n = 0;

    if (!ReadFile((HANDLE)this->_fd, buffer.data(), (DWORD)buffer.size(), &n, nullptr)) {
        DWORD error = GetLastError();
        switch (error) {
            case ERROR_BROKEN_PIPE: return IOResult::failure(IOResult::Error::EndOfFile, 0);
            case ERROR_NO_DATA: return IOResult::failure(IOResult::Error::WouldBlock, 0);
            default:
                unrecoverableError({
                    Error::ErrorCode::CoreBug,
                    "Originated from",
                    "ReadFile()",
                });
        }
    }

    return IOResult::success(n);
}

}  // namespace pipe
}  // namespace utility
}  // namespace scaler

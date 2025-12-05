#include <Windows.h>

#include <cstddef>
#include <cstdint>
#include <span>

#include "scaler/utility/error.h"
#include "scaler/utility/io_result.h"
#include "scaler/utility/pipe/pipe_writer.h"

namespace scaler {
namespace utility {
namespace pipe {

PipeWriter::~PipeWriter()
{
    if (this->_fd == -1) {
        return;
    }

    CloseHandle((HANDLE)this->_fd);
}

IOResult PipeWriter::write(std::span<const uint8_t> buffer) const noexcept
{
    DWORD n = 0;

    if (!WriteFile((HANDLE)this->_fd, buffer.data(), (DWORD)buffer.size(), &n, nullptr)) {
        DWORD error = GetLastError();
        switch (error) {
            case ERROR_NO_DATA: return IOResult::failure(IOResult::Error::WouldBlock, 0);
            default:
                unrecoverableError({
                    Error::ErrorCode::CoreBug,
                    "Originated from",
                    "WriteFile()",
                    "Error is",
                    std::to_string(error),
                });
        }
    }

    return IOResult::success(bytes_written);
}

}  // namespace pipe
}  // namespace utility
}  // namespace scaler

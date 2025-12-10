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

IOResult PipeWriter::writeBytes(const std::vector<std::span<const uint8_t>>& buffers) const noexcept
{
    size_t bytesTransferred = 0;

    for (const auto& buffer: buffers) {
        DWORD n = 0;

        if (!WriteFile((HANDLE)this->_fd, buffer.data(), (DWORD)buffer.size(), &n, nullptr)) {
            DWORD error = GetLastError();
            switch (error) {
                case ERROR_NO_DATA: return IOResult::failure(IOResult::Error::WouldBlock, bytesTransferred + n);
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

        bytesTransferred += n;
    }

    return IOResult::success(bytesTransferred);
}

}  // namespace pipe
}  // namespace utility
}  // namespace scaler

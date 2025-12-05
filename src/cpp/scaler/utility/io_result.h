#pragma once

#include <optional>

namespace scaler {
namespace utility {

// The result of a possibly non-blocking I/O action.
struct IOResult {
    enum class Error { WouldBlock, EndOfFile };

    std::optional<Error> error;
    size_t bytesTransferred;

    static IOResult success(size_t bytesTransferred = 0) { return IOResult(std::nullopt, bytesTransferred); }

    static IOResult failure(Error error, size_t bytesTransferred = 0) { return IOResult(error, bytesTransferred); }
};

}  // namespace utility
}  // namespace scaler

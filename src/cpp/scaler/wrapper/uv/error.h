#pragma once

#include <uv.h>

#include <expected>
#include <string>
#include <utility>

#include "scaler/error/error.h"

namespace scaler {
namespace wrapper {
namespace uv {

struct Error {
    // See UV_xxx error codes.
    constexpr Error(int code) noexcept: _code(code) {}

    int code() const noexcept;

    // See uv_err_name
    std::string name() const noexcept;

    // See uv_strerror
    std::string message() const noexcept;

    constexpr bool operator==(const Error&) const noexcept = default;
    constexpr bool operator!=(const Error&) const noexcept = default;

    // See uv_translate_sys_error
    static Error fromSysError(int systemErrorCode) noexcept;

private:
    int _code;
};

// Validate the given std::expected<T, Error> expression, triggering an unrecoverableError() on failure.
//
// For example:
//
//     Loop loop = UV_EXIT_ON_ERROR(Loop::init(loopFlags));
//
#define UV_EXIT_ON_ERROR(expr) scaler::wrapper::uv::_exitOnError(#expr, std::move(expr))

template <typename T>
T _exitOnError(std::string_view functionName, std::expected<T, Error>&& result)
{
    if (!result.has_value()) {
        uv::Error uvError = result.error();

        unrecoverableError({
            ymq::Error::ErrorCode::UVError,
            "Originated from",
            std::string(functionName),
            "Error code",
            uvError.name(),
            uvError.message(),
        });

        std::unreachable();
    }

    if constexpr (!std::is_void_v<T>) {
        return std::move(result.value());
    }
}

}  // namespace uv
}  // namespace wrapper
}  // namespace scaler

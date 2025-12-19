#pragma once

#include <uv.h>

#include <string>

namespace scaler {
namespace uv {

struct Error {
    int code;

    // See UV_xxx error codes.
    constexpr Error(int code): code(code) {}

    // See uv_err_name
    std::string name() const noexcept;

    // See uv_strerror
    std::string message() const noexcept;

    constexpr bool operator==(const Error&) const noexcept = default;
    constexpr bool operator!=(const Error&) const noexcept = default;

    // See uv_translate_sys_error
    static Error fromSysError(int systemErrorCode) noexcept;
};

}  // namespace uv
}  // namespace scaler

#pragma once

#include <uv.h>

#include <string>

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

}  // namespace uv
}  // namespace wrapper
}  // namespace scaler

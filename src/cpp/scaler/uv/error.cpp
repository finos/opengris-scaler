#include "scaler/uv/error.h"

namespace scaler {
namespace uv {

int Error::code() const noexcept
{
    return _code;
}

std::string Error::name() const noexcept
{
    return uv_err_name(code());
}

std::string Error::message() const noexcept
{
    return uv_strerror(code());
}

Error Error::fromSysError(int systemErrorCode) noexcept
{
    return {uv_translate_sys_error(systemErrorCode)};
}

}  // namespace uv
}  // namespace scaler

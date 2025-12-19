#include "scaler/uv/error.h"

#include <uv.h>

#include <string>

namespace scaler {
namespace uv {

std::string Error::name() const noexcept
{
    return uv_err_name(code);
}

std::string Error::message() const noexcept
{
    return uv_strerror(code);
}

Error Error::fromSysError(int systemErrorCode) noexcept
{
    return {uv_translate_sys_error(systemErrorCode)};
}

}  // namespace uv
}  // namespace scaler

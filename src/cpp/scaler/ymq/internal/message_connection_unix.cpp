#include "scaler/ymq/internal/message_connection.h"

namespace scaler {
namespace ymq {
namespace internal {

bool MessageConnection::isAbortConsumedByWrite(const scaler::wrapper::uv::Error& error) noexcept
{
    // Linux hands a reset to the first write, leaving EOF to the read. A write after a graceful close gets EPIPE.
    return isConnectionError(error) && error.code() != UV_EPIPE;
}

}  // namespace internal
}  // namespace ymq
}  // namespace scaler

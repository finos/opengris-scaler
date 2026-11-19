#include "scaler/ymq/internal/message_connection.h"

namespace scaler {
namespace ymq {
namespace internal {

bool MessageConnection::isAbortConsumedByWrite([[maybe_unused]] const scaler::wrapper::uv::Error& error) noexcept
{
    // Winsock reports a reset to every read, and can fail a write after a graceful close with ECONNRESET.
    return false;
}

}  // namespace internal
}  // namespace ymq
}  // namespace scaler

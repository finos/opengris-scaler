#pragma once

#include <cstdint>
#include <utility>

namespace scaler {
namespace utility {
namespace pipe {

// create platform-specific pipe handles
// the first handle is read, the second handle is write
std::pair<int64_t, int64_t> createPipe();

// set the handle to non-blocking mode
void setNonBlocking(int64_t handle);

}  // namespace pipe
}  // namespace utility
}  // namespace scaler

#pragma once

#include <cstdint>
#include <utility>

namespace scaler {
namespace utility {
namespace pipe {

// create platform-specific pipe handles
// the first handle is read, the second handle is write
std::pair<int64_t, int64_t> create_pipe();

}  // namespace pipe
}  // namespace utility
}  // namespace scaler

#pragma once

#include <utility>

namespace scaler {
namespace utility {
namespace pipe {

// create platform-specific pipe handles
// the first handle is read, the second handle is write
std::pair<long long, long long> create_pipe();

}  // namespace pipe
}  // namespace utility
}  // namespace scaler

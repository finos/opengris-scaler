#pragma once

namespace scaler {
namespace object_storage {

static constexpr const size_t memoryLimitInBytes = 6uz << 40;  // 6 TB
static constexpr const char* defaultAddr         = "127.0.0.1";
static constexpr const char* defaultPort         = "55555";

};  // namespace object_storage
};  // namespace scaler

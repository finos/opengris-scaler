#pragma once

namespace scaler {
namespace object_storage {

static constexpr size_t MEMORY_LIMIT_IN_BYTES = 6uz << 40;  // 6 TB
static constexpr const char* DEFAULT_ADDR     = "127.0.0.1";
static constexpr const char* DEFAULT_PORT     = "55555";

};  // namespace object_storage
};  // namespace scaler

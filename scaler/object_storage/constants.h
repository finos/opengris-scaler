#pragma once

#include <capnp/common.h>

#include <cstddef>
#include <string_view>

namespace scaler {
namespace object_storage {

static constexpr size_t CAPNP_HEADER_SIZE     = 80;
static constexpr size_t CAPNP_WORD_SIZE       = sizeof(capnp::word);
static constexpr size_t MEMORY_LIMIT_IN_BYTES = 6uz << 40;  // 6 TB
static constexpr const char* DEFAULT_ADDR     = "127.0.0.1";
static constexpr const char* DEFAULT_PORT     = "55555";

};  // namespace object_storage
};  // namespace scaler

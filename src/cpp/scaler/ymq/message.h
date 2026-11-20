
#pragma once

#include <cstdint>
#include <memory>

#include "scaler/ymq/bytes.h"

namespace scaler {
namespace ymq {

struct Message {
    std::unique_ptr<Bytes> address;
    std::unique_ptr<Bytes> payload;

    // The connection that delivered this message to a BinderSocket: a remote that reconnects gets a new one.
    uint64_t connectionID {0};
};

}  // namespace ymq
}  // namespace scaler

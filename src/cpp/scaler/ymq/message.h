
#pragma once

#include <memory>

#include "scaler/ymq/bytes.h"

namespace scaler {
namespace ymq {

struct Message {
    std::unique_ptr<Bytes> address;
    std::unique_ptr<Bytes> payload;
};

}  // namespace ymq
}  // namespace scaler

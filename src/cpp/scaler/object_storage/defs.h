#pragma once

#include <memory>  // std::shared_ptr

#include "scaler/ymq/bytes.h"

namespace scaler {
namespace object_storage {

using ObjectPayload       = Bytes;
using SharedObjectPayload = std::shared_ptr<ObjectPayload>;

};  // namespace object_storage
};  // namespace scaler

#pragma once

#include <memory>  // std::shared_ptr
#include <vector>

#include "scaler/io/ymq/bytes.h"

namespace scaler {
namespace object_storage {

using ObjectPayload       = Bytes;
using SharedObjectPayload = std::shared_ptr<ObjectPayload>;

template <typename T, typename... U>
concept Either = (std::same_as<T, U> || ...);

template <typename T, typename K>
concept PairOrVecOf = Either<std::remove_cvref_t<T>, std::pair<K, K>, std::vector<K>>;

};  // namespace object_storage
};  // namespace scaler

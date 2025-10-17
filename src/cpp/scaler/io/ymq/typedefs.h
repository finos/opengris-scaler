#pragma once

#include <cstdint>

namespace scaler {
namespace ymq {

// THIS FILE MUST NOT CONTAIN USER DEFINED TYPES
enum IOSocketType : uint8_t {
    Uninit,  // Not allowed from user code
    Binder,
    Connector,
    Unicast,
    Multicast,
};

enum Ownership { Owned, Borrowed };

}  // namespace ymq
}  // namespace scaler

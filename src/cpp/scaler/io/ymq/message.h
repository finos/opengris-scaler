
#pragma once

#include "scaler/io/ymq/bytes.h"

namespace scaler {
namespace ymq {

struct Message {
    Bytes address;  // Address of the message
    Bytes payload;  // Payload of the message
};

}  // namespace ymq
}  // namespace scaler


#pragma once

#include "scaler/io/ymq/bytes.h"

struct Message {
    Bytes address;  // Address of the message
    Bytes payload;  // Payload of the message
};

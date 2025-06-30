#pragma once

#include <cstdint>

// THIS FILE MUST NOT CONTAIN USER DEFINED TYPES
enum IOSocketType : uint8_t { Uninit, Binder, Sub, Pub, Dealer, Router, Pair /* etc. */ };
enum Ownership { Owned, Borrowed };

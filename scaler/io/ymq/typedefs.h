#pragma once

#include <cstdint>

enum IOSocketType : uint8_t { Uninit, Binder, Sub, Pub, Dealer, Router, Pair /* etc. */ };
enum Ownership { Owned, Borrowed };

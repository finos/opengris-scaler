#pragma once

// First-party
#include "scaler/io/ymq/epoll_context.h"

struct configuration {
    using polling_context_t = EpollContext;
};

#pragma once

// C++
#include <string>

class EpollContext;

struct Configuration {
    using PollingContext = EpollContext;
    using Identity = std::string;
};

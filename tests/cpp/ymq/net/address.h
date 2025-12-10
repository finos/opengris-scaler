#pragma once

#include <cstdint>
#include <string>

struct Address {
    std::string protocol;
    std::string host;
    uint16_t port;
    std::string path;
};

Address parseAddress(const std::string& address_str);

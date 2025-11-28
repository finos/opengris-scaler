#include "tests/cpp/ymq/net/address.h"

#include <stdexcept>

Address parseAddress(const std::string& address_str)
{
    if (address_str.rfind("tcp://", 0) == 0) {  // Check if string starts with "tcp://"
        std::string_view remaining = address_str;
        remaining.remove_prefix(6);  // Remove "tcp://"

        size_t colon_pos = remaining.find(':');
        if (colon_pos == std::string_view::npos) {
            throw std::runtime_error("Invalid TCP address format: missing port");
        }

        std::string host(remaining.substr(0, colon_pos));
        uint16_t port = (uint16_t)std::stoi(std::string(remaining.substr(colon_pos + 1)));
        return Address {"tcp", host, port, ""};
    }

    if (address_str.rfind("ipc://", 0) == 0) {  // Check if string starts with "ipc://"
        std::string_view remaining = address_str;
        remaining.remove_prefix(6);  // Remove "ipc://"
        std::string path(remaining);
        return Address {"ipc", "", 0, path};
    }

    throw std::runtime_error("Invalid address format: " + address_str);
}

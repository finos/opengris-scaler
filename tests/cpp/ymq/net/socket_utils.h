#include <memory>

#include "tests/cpp/ymq/net/socket.h"

std::unique_ptr<Socket> connect_socket(std::string& address_str);
std::unique_ptr<Socket> bind_socket(std::string& address_str);

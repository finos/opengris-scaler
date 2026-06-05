#include <memory>

#include "tests/cpp/ymq/net/socket.h"

std::unique_ptr<Socket> connectSocket(std::string& address_str);
std::unique_ptr<Socket> bindSocket(std::string& address_str);

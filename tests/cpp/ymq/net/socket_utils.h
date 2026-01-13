#include <memory>

#include "tests/cpp/ymq/net/i_socket.h"

std::unique_ptr<ISocket> connect_socket(std::string& address_str);
std::unique_ptr<ISocket> bind_socket(std::string& address_str);

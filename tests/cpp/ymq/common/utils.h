#include <cstdint>
#include <string>

// throw an error with the last system error code
void raise_system_error(const char* msg);

// throw wan error with the last socket error code
void raise_socket_error(const char* msg);

// returns its input unless it matches "localhost" in which case 127.0.0.1 is returned
const char* check_localhost(const char* host);

// formats a tcp:// address
std::string format_address(std::string host, uint16_t port);

// change the current working directory to the project root
// this is important for finding the python mitm script
void chdir_to_project_root();

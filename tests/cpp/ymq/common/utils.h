#pragma once

#include <cstdint>
#include <string>

// throw an error with the last system error code
void raise_system_error(const char* msg);

// throw wan error with the last socket error code
void raise_socket_error(const char* msg);

// change the current working directory to the project root
// this is important for finding the python mitm script
void chdir_to_project_root();

unsigned short random_port(unsigned short min_port = 1024, unsigned short max_port = 65535);

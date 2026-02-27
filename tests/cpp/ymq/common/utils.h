#pragma once

#include <cstdint>
#include <string>

// throw an error with the last system error code
void raiseSystemError(const char* msg);

// throw wan error with the last socket error code
void raiseSocketError(const char* msg);

// change the current working directory to the project root
// this is important for finding the python mitm script
void chdirToProjectRoot();

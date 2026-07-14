#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "scaler/ymq/tls_config.h"

// throw an error with the last system error code
void raiseSystemError(const char* msg);

// throw wan error with the last socket error code
void raiseSocketError(const char* msg);

// change the current working directory to the project root
// this is important for finding the python mitm script
void chdirToProjectRoot();

// Return the list of transports to parameterize the socket test suites with.
std::vector<std::string> getTransports();

// Build an address string for the given transport ("tcp", "tls", "ipc", "ws" or "wss").
std::string getTransportAddress(const std::string& transport, int port);

// Return an OS-assigned free TCP port, so repeated or concurrent test runs do not collide on a
// fixed port. Delegates to scaler::object_storage::getAvailableTCPPort(); there is a small TOCTOU
// window between probing the port and binding it.
int getFreePort();

// Return a TLSConfig for secure transports, or std::nullopt otherwise.
std::optional<scaler::ymq::TLSConfig> getTLSConfig(const std::string& transport);

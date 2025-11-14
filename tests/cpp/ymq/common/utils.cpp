#include "tests/cpp/ymq/common/utils.h"

#include <cstring>
#include <filesystem>
#include <random>
#include <sstream>

#ifdef __linux__
#include <errno.h>
#endif  // __linux__
#ifdef _WIN32
#include <Windows.h>
#include <winsock2.h>
#endif  // _WIN32

void raise_system_error(const char* msg)
{
#ifdef __linux__
    throw std::system_error(errno, std::generic_category(), msg);
#endif  // __linux__
#ifdef _WIN32
    throw std::system_error(GetLastError(), std::generic_category(), msg);
#endif  // _WIN32
}

void raise_socket_error(const char* msg)
{
#ifdef __linux__
    throw std::system_error(errno, std::generic_category(), msg);
#endif  // __linux__
#ifdef _WIN32
    throw std::system_error(WSAGetLastError(), std::generic_category(), msg);
#endif  // _WIN32
}

const char* check_localhost(const char* host)
{
    return std::strcmp(host, "localhost") == 0 ? "127.0.0.1" : host;
}

std::string format_address(std::string host, uint16_t port)
{
    std::ostringstream oss;
    oss << "tcp://" << check_localhost(host.c_str()) << ":" << port;
    return oss.str();
}

// change the current working directory to the project root
// this is important for finding the python mitm script
void chdir_to_project_root()
{
    auto cwd = std::filesystem::current_path();

    // if pyproject.toml is in `path`, it's the project root
    for (auto path = cwd; !path.empty(); path = path.parent_path()) {
        if (std::filesystem::exists(path / "pyproject.toml")) {
            // change to the project root
            std::filesystem::current_path(path);
            return;
        }
    }
}

unsigned short random_port(unsigned short min_port, unsigned short max_port)
{
    static thread_local std::mt19937_64 rng(std::random_device {}());
    std::uniform_int_distribution<unsigned int> dist(min_port, max_port);
    return static_cast<unsigned short>(dist(rng));
}

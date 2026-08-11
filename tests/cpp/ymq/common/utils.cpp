#include "tests/cpp/ymq/common/utils.h"

#include <cstdlib>
#include <filesystem>
#include <format>
#include <stdexcept>
#include <string>
#include <vector>

// change the current working directory to the project root
// this is important for finding the python mitm script
void chdirToProjectRoot()
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

std::vector<std::string> getTransports()
{
    std::vector<std::string> transports;
    transports.push_back("tcp");
    transports.push_back("tls");
    transports.push_back("ws");
    transports.push_back("wss");
#ifdef __linux__
    transports.push_back("ipc");
#endif
    return transports;
}

std::string getTransportAddress(const std::string& transport, int port)
{
    if (transport == "tcp") {
        return std::format("tcp://127.0.0.1:{}", port);
    }
    if (transport == "tls") {
        return std::format("tls://127.0.0.1:{}", port);
    }
    if (transport == "ipc") {
        const char* runnerTemp = std::getenv("RUNNER_TEMP");
        if (runnerTemp) {
            return std::format("ipc://{}/ymq-test-{}.ipc", runnerTemp, port);
        }
        return std::format("ipc:///tmp/ymq-test-{}.ipc", port);
    }
    if (transport == "ws") {
        return std::format("ws://127.0.0.1:{}/", port);
    }
    if (transport == "wss") {
        return std::format("wss://127.0.0.1:{}/", port);
    }

    throw std::invalid_argument("invalid transport");
}

std::optional<scaler::ymq::TLSConfig> getTLSConfig(const std::string& transport)
{
    if (transport == "tls" || transport == "wss") {
        return scaler::ymq::TLSConfig {
            "../wrapper/openssl/sample_cert.pem", "../wrapper/openssl/sample_private_key.pem"};
    }
    return std::nullopt;
}

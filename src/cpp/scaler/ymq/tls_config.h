#pragma once

#include <expected>
#include <string>

#include "scaler/error/error.h"
#include "scaler/wrapper/openssl/ssl_context.h"

namespace scaler {
namespace ymq {

// TLS credentials for secure connections (tls://, wss://).
class TLSConfig {
public:
    TLSConfig(std::string certChain, std::string privateKey) noexcept;

    // Create a SSLContext from the stored credentials.
    std::expected<scaler::wrapper::openssl::SSLContext, Error> getSSLContext() const noexcept;

private:
    std::string _certChain;   // PEM certificate chain file
    std::string _privateKey;  // PEM private key file
};

}  // namespace ymq
}  // namespace scaler

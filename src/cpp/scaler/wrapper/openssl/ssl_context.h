#pragma once

#include <openssl/ssl.h>

#include <expected>
#include <memory>
#include <string>

#include "scaler/wrapper/openssl/types.h"
#include "scaler/wrapper/uv/error.h"

namespace scaler {
namespace wrapper {
namespace openssl {

// A RAII wrapper over OpenSSL's SSL_CTX
class SSLContext {
public:
    inline static const SSL_METHOD* defaultSSLMethod = TLS_method();

    // See SSL_CTX_new
    static std::expected<SSLContext, uv::Error> init(const SSL_METHOD* method = defaultSSLMethod) noexcept;

    // See SSL_CTX_use_certificate_chain_file
    std::expected<void, uv::Error> useCertificateChainFile(const std::string& path) noexcept;

    // See SSL_CTX_use_PrivateKey_file
    std::expected<void, uv::Error> usePrivateKeyFile(const std::string& path, int type = SSL_FILETYPE_PEM) noexcept;

    // See SSL_CTX_check_private_key
    std::expected<void, uv::Error> checkPrivateKey() const noexcept;

    // Access the underlying SSL_CTX pointer (non-owning).
    SSL_CTX* native() const noexcept;

private:
    SSLContext(std::shared_ptr<SSL_CTX> context) noexcept;

    std::shared_ptr<SSL_CTX> _context;
};

}  // namespace openssl
}  // namespace wrapper
}  // namespace scaler

#include "scaler/wrapper/openssl/ssl_context.h"

namespace scaler {
namespace wrapper {
namespace openssl {

std::expected<SSLContext, uv::Error> SSLContext::init(const SSL_METHOD* method) noexcept
{
    std::shared_ptr<SSL_CTX> context {SSL_CTX_new(method), SSL_CTX_free};
    if (context == nullptr) {
        return std::unexpected {uv::Error {UV_ENOMEM}};
    }

    return SSLContext {std::move(context)};
}

SSLContext::SSLContext(std::shared_ptr<SSL_CTX> context) noexcept: _context(std::move(context))
{
}

std::expected<void, uv::Error> SSLContext::useCertificateChainFile(const std::string& path) noexcept
{
    if (SSL_CTX_use_certificate_chain_file(_context.get(), path.c_str()) != 1) {
        return std::unexpected {uv::Error {UV_EPROTO}};
    }
    return {};
}

std::expected<void, uv::Error> SSLContext::usePrivateKeyFile(const std::string& path, int type) noexcept
{
    if (SSL_CTX_use_PrivateKey_file(_context.get(), path.c_str(), type) != 1) {
        return std::unexpected {uv::Error {UV_EPROTO}};
    }
    return {};
}

std::expected<void, uv::Error> SSLContext::checkPrivateKey() const noexcept
{
    if (SSL_CTX_check_private_key(_context.get()) != 1) {
        return std::unexpected {uv::Error {UV_EPROTO}};
    }
    return {};
}

SSL_CTX* SSLContext::native() const noexcept
{
    return _context.get();
}

}  // namespace openssl
}  // namespace wrapper
}  // namespace scaler

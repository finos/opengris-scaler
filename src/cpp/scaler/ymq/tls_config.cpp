#include "scaler/ymq/tls_config.h"

#include <utility>

namespace scaler {
namespace ymq {

TLSConfig::TLSConfig(std::string certChain, std::string privateKey) noexcept
    : _certChain(std::move(certChain)), _privateKey(std::move(privateKey))
{
}

std::expected<scaler::wrapper::openssl::SSLContext, Error> TLSConfig::getSSLContext() const noexcept
{
    auto context = scaler::wrapper::openssl::SSLContext::init();
    if (!context.has_value()) {
        return std::unexpected {Error {Error::ErrorCode::SysCallError, "SSLContext::init() failed"}};
    }

    auto certResult = context->useCertificateChainFile(_certChain);
    if (!certResult.has_value()) {
        return std::unexpected {
            Error {Error::ErrorCode::SysCallError, "Failed to load certificate chain file", _certChain}};
    }

    auto keyResult = context->usePrivateKeyFile(_privateKey);
    if (!keyResult.has_value()) {
        return std::unexpected {Error {Error::ErrorCode::SysCallError, "Failed to load private key file", _privateKey}};
    }

    auto checkResult = context->checkPrivateKey();
    if (!checkResult.has_value()) {
        return std::unexpected {Error {Error::ErrorCode::SysCallError, "Private key does not match certificate"}};
    }

    return std::move(*context);
}

}  // namespace ymq
}  // namespace scaler

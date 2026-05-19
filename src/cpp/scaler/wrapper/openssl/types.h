#pragma once

#include <openssl/bio.h>
#include <openssl/ssl.h>

#include <memory>

namespace scaler {
namespace wrapper {
namespace openssl {

template <typename T>
struct SSLDeleter;

// A unique_ptr alias specialized for SSL datastructures.
//
//     SSLPtr<SSL> ssl { SSL_new(...) };
//
template <typename T>
using SSLPtr = std::unique_ptr<T, SSLDeleter<T>>;

template <>
struct SSLDeleter<SSL_CTX> {
    void operator()(SSL_CTX* ptr) const noexcept
    {
        SSL_CTX_free(ptr);
    }
};

template <>
struct SSLDeleter<SSL> {
    void operator()(SSL* ptr) const noexcept
    {
        SSL_free(ptr);
    }
};

template <>
struct SSLDeleter<BIO> {
    void operator()(BIO* ptr) const noexcept
    {
        BIO_free(ptr);
    }
};

}  // namespace openssl
}  // namespace wrapper
}  // namespace scaler

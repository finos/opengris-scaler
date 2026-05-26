#pragma once

#include <cstddef>
#include <cstdint>

namespace scaler {
namespace ymq {

class Bytes {
public:
    virtual ~Bytes() noexcept = 0;

    Bytes()                            = default;
    Bytes(Bytes&&) noexcept            = default;
    Bytes& operator=(Bytes&&) noexcept = default;
    Bytes(const Bytes&)                = delete;
    Bytes& operator=(const Bytes&)     = delete;

    virtual const uint8_t* data() const noexcept = 0;
    virtual uint8_t* data() noexcept             = 0;
    virtual size_t size() const noexcept         = 0;
};

inline Bytes::~Bytes() noexcept = default;

}  // namespace ymq
}  // namespace scaler

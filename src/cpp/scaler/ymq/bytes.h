#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>

namespace scaler {
namespace ymq {

class Bytes {
public:
    virtual ~Bytes() noexcept = default;

    Bytes()                        = default;
    Bytes(Bytes&&)                 = default;
    Bytes& operator=(Bytes&&)      = default;
    Bytes(const Bytes&)            = default;
    Bytes& operator=(const Bytes&) = default;

    virtual const uint8_t* data() const noexcept        = 0;
    virtual uint8_t* data() noexcept                    = 0;
    virtual size_t size() const noexcept                = 0;
    virtual std::optional<std::string> asString() const = 0;
};

}  // namespace ymq
}  // namespace scaler

#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>

#include "scaler/ymq/bytes.h"

namespace scaler {
namespace ymq {

class BufferedBytes final: public Bytes {
public:
    explicit BufferedBytes(size_t size): _data(std::make_unique<uint8_t[]>(size)), _size(size)
    {
    }

    BufferedBytes(const char* src, size_t size): _data(std::make_unique<uint8_t[]>(size)), _size(size)
    {
        std::memcpy(_data.get(), src, size);
    }

    explicit BufferedBytes(const char* src): BufferedBytes(src, src ? std::strlen(src) : 0)
    {
    }

    explicit BufferedBytes(const std::string& s): BufferedBytes(s.data(), s.size())
    {
    }

    BufferedBytes(BufferedBytes&&) noexcept            = default;
    BufferedBytes& operator=(BufferedBytes&&) noexcept = default;

    const uint8_t* data() const noexcept override
    {
        return _data.get();
    }
    uint8_t* data() noexcept override
    {
        return _data.get();
    }
    size_t size() const noexcept override
    {
        return _size;
    }

    bool is_null() const noexcept
    {
        return _data == nullptr;
    }

private:
    std::unique_ptr<uint8_t[]> _data;
    size_t _size {0};
};

}  // namespace ymq
}  // namespace scaler

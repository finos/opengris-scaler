#pragma once

// C
#include <string.h>  // memcmp

#include <algorithm>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <cstring>

// C++
#include <optional>
#include <string>

// First-party
#include "scaler/ymq/common.h"

// TODO: This is not in the namespace
class Bytes {
    uint8_t* _data;
    size_t _len;

    void free()
    {
        if (is_null())
            return;
        delete[] _data;
        _data = nullptr;
    }

    explicit Bytes(uint8_t* data, size_t len): _data(data), _len(len) {}

public:
    Bytes(char* data, size_t len): _data(datadup((uint8_t*)data, len)), _len(len) {}

    explicit Bytes(const std::string& s): _data(datadup((uint8_t*)s.data(), s.length())), _len(s.length()) {}

    Bytes(): _data {}, _len {} {}

    Bytes(const Bytes& other) noexcept
    {
        this->_data = datadup(other._data, other._len);
        this->_len  = other._len;
    }

    Bytes& operator=(const Bytes& other) noexcept
    {
        Bytes tmp(other);
        swap(*this, tmp);
        return *this;
    }

    friend void swap(Bytes& x, Bytes& y) noexcept
    {
        using std::swap;
        swap(x._len, y._len);
        swap(x._data, y._data);
    }

    Bytes(Bytes&& other) noexcept: _data(other._data), _len(other._len)
    {
        other._data = nullptr;
        other._len  = 0;
    }

    friend std::strong_ordering operator<=>(const Bytes& x, const Bytes& y) noexcept
    {
        return std::lexicographical_compare_three_way(x._data, x._data + x._len, y._data, y._data + y._len);
    }

    // https://stackoverflow.com/questions/68221024/why-must-i-provide-operator-when-operator-is-enough
    friend bool operator==(const Bytes& x, const Bytes& y) noexcept { return x <=> y == 0; }

    Bytes& operator=(Bytes&& other) noexcept
    {
        if (this != &other) {
            this->free();  // free current data

            _data = other._data;
            _len  = other._len;

            other._data = nullptr;
            other._len  = 0;
        }
        return *this;
    }

    ~Bytes() { this->free(); }

    [[nodiscard]] constexpr bool operator!() const noexcept { return is_null(); }

    [[nodiscard]] constexpr bool is_null() const noexcept { return !this->_data; }

    std::optional<std::string> as_string() const
    {
        if (is_null())
            return std::nullopt;

        return std::string((char*)_data, _len);
    }

    [[nodiscard("Allocated Bytes is not used, likely causing a memory leak")]]
    static Bytes alloc(size_t len)
    {
        auto ptr = new uint8_t[len];  // we just assume the allocation will succeed
        return Bytes {ptr, len};
    }

    [[nodiscard]] constexpr size_t len() const { return _len; }
    [[nodiscard]] constexpr size_t size() const { return _len; }
    [[nodiscard]] constexpr const uint8_t* data() const { return _data; }
    [[nodiscard]] constexpr uint8_t* data() { return _data; }
};

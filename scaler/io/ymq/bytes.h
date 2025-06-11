#pragma once

// C
#include <cstddef>
#include <cstdint>
#include <cstring>

// C++
#include <string>

// First-party
#include "scaler/io/ymq/common.h"
#include "scaler/io/ymq/typedefs.h"

class Bytes {
    uint8_t* _data;
    size_t _len;
    Ownership _tag;

    void free() {
        if (_tag != Owned)
            return;

        if (is_empty())
            return;

        delete[] _data;
        this->_data = NULL;
    }

    Bytes(uint8_t* m_data, size_t m_len, Ownership tag): _data(m_data), _len(m_len), _tag(tag) {}

public:
    // move-only
    // TODO: make copyable
    Bytes(const Bytes&)            = delete;
    Bytes& operator=(const Bytes&) = delete;
    Bytes(Bytes&& other) noexcept: _data(other._data), _len(other._len), _tag(other._tag) {
        other._data = NULL;
        other._len  = 0;
    }
    Bytes& operator=(Bytes&& other) noexcept {
        if (this != &other) {
            this->free();  // free current data

            _data = other._data;
            _len  = other._len;
            _tag    = other._tag;

            other._data = NULL;
            other._len  = 0;
        }
        return *this;
    }

    ~Bytes() { this->free(); }

    bool operator==(const Bytes& other) const {
        if (_len != other._len)
            return false;

        if (_data == other._data)
            return true;

        return std::memcmp(_data, other._data, _len) == 0;
    }

    bool operator!() const { return is_empty(); }

    bool is_empty() const { return this->_data == NULL; }

    // debugging utility
    std::string as_string() const {
        if (is_empty())
            return "[EMPTY]";

        return std::string((char*)_data, _len);
    }

    Bytes ref() { return Bytes {this->_data, this->_len, Borrowed}; }

    static Bytes alloc(size_t m_len) {
        if (m_len == 0)
            return empty();

        return Bytes {new uint8_t[m_len], m_len, Owned};
    }

    static Bytes empty() { return Bytes {NULL, 0, Owned}; }

    static Bytes copy(const uint8_t* m_data, size_t m_len) {
        if (m_len == 0)
            return empty();

        return Bytes {datadup(m_data, m_len), m_len, Owned};
    }

    static Bytes clone(const Bytes& bytes) {
        if (bytes.is_empty())
            panic("tried to clone empty bytes");

        return Bytes {datadup(bytes._data, bytes._len), bytes._len, Owned};
    }

    // static Bytes from_buffer(Buffer& buffer) { return buffer.into_bytes(); }

    // // consume this Bytes and return a Buffer object
    // Buffer into_buffer() {
    //     if (tag != Owned) {
    //         // if the m_data is borrowed, we need to copy it
    //         auto new_m_data = new uint8_t[m_len];
    //         std::memcpy(new_m_data, m_data, m_len);
    //         m_data = new_m_data;
    //         tag    = Owned;  // now we own the m_data
    //     }

    //     Buffer buffer {m_data, m_len, m_len};
    //     m_data = NULL;  // prevent double free
    //     m_len  = 0;     // prevent double free
    //     return buffer;
    // }

    size_t len() const { return _len; }
    const uint8_t* data() const { return _data; }

    friend class Buffer;
};

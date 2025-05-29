#pragma once

// C
#include <cstddef>
#include <cstdint>
#include <cstring>

// C++
#include <string>

// First-party
#include "common.hpp"

class Bytes {
    uint8_t* m_data;
    size_t m_len;

    enum Ownership { Owned, Borrowed } tag;

    void free() {
        if (tag != Owned)
            return;

        if (is_empty())
            return;

        delete[] m_data;
        this->m_data = NULL;
    }

    Bytes(uint8_t* m_data, size_t m_len, Ownership tag): m_data(m_data), m_len(m_len), tag(tag) {
        if (tag == Owned && m_data == NULL)
            panic("tried to create owned bytes with NULL m_data");
    }

public:
    // move-only
    // TODO: make copyable
    Bytes(const Bytes&)            = delete;
    Bytes& operator=(const Bytes&) = delete;
    Bytes(Bytes&& other) noexcept: m_data(other.m_data), m_len(other.m_len), tag(other.tag) {
        other.m_data = NULL;
        other.m_len  = 0;
    }
    Bytes& operator=(Bytes&& other) noexcept {
        if (this != &other) {
            this->free();  // free current data

            m_data = other.m_data;
            m_len  = other.m_len;
            tag    = other.tag;

            other.m_data = NULL;
            other.m_len  = 0;
        }
        return *this;
    }

    ~Bytes() { this->free(); }

    bool operator==(const Bytes& other) const {
        if (m_len != other.m_len)
            return false;

        if (m_data == other.m_data)
            return true;

        return std::memcmp(m_data, other.m_data, m_len) == 0;
    }

    bool operator!() const { return is_empty(); }

    bool is_empty() const { return this->m_data == NULL; }

    // debugging utility
    std::string as_string() const {
        if (is_empty())
            return "[EMPTY]";

        return std::string((char*)m_data, m_len);
    }

    Bytes ref() { return Bytes {this->m_data, this->m_len, Borrowed}; }

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

        return Bytes {datadup(bytes.m_data, bytes.m_len), bytes.m_len, Owned};
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

    size_t len() const { return m_len; }

    friend class Buffer;
};

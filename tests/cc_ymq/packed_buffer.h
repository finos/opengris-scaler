#include <netinet/in.h>

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <optional>
#include <stdexcept>
#include <vector>

inline bool host_is_network_byte_order()
{
    uint16_t x = 0x00FF;

    // if the lowest byte is 0, host is big endian
    return (*((uint8_t*)&x)) == 0x00;
}

// a packed buffer of bytes
struct PackedBuffer {
    size_t cursor;
    std::vector<uint8_t> buffer;

    // -- constructors

    PackedBuffer(size_t size): cursor(0), buffer(size, 0) {}
    PackedBuffer(std::vector<uint8_t> buffer): cursor(0), buffer(buffer) {}
    PackedBuffer(uint8_t* buffer, size_t size): cursor(0), buffer(buffer, buffer + size) {}

    // util
    void check_space(size_t advance)
    {
        if (this->cursor + advance > this->buffer.size())
            throw std::runtime_error("tried to read/write past the end of the buffer");
    }

    size_t remaining() { return this->buffer.size() - this->cursor; }

    void seek(size_t pos) { this->cursor = pos; }
    void reset() { this->seek(0); }

    uint8_t* at(size_t idx) { return this->buffer.data() + idx; }

    uint16_t ipv4_checksum()
    {
        uint64_t sum = 0;
        for (size_t i = 0; i < this->buffer.size(); i += 2)
            if (i + 1 >= this->buffer.size())
                sum += this->buffer[i] << 8;
            else
                sum += (this->buffer[i] << 8) | this->buffer[i + 1];
        while (sum >> 16)
            sum = (sum & 0xffff) + (sum >> 16);
        return ~sum;
    }

    // -- reading
    uint8_t read_u8()
    {
        this->check_space(1);
        auto x = this->buffer[cursor];
        this->cursor++;
        return x;
    }

    uint16_t read_u16()
    {
        this->check_space(2);
        uint16_t x = 0;
        std::memcpy(&x, this->at(cursor), 2);
        this->cursor += 2;
        return ntohs(x);
    }

    uint32_t read_u32()
    {
        this->check_space(4);
        uint32_t x = 0;
        std::memcpy(&x, this->at(cursor), 4);
        this->cursor += 4;
        return ntohl(x);
    }

    std::vector<uint8_t> read_n(size_t n)
    {
        this->check_space(n);

        std::vector<uint8_t> vec(n, 0);
        if (host_is_network_byte_order())
            std::copy(this->at(cursor), this->at(cursor + n), &vec[0]);
        else
            std::reverse_copy(this->at(cursor), this->at(cursor + n), &vec[0]);
        this->cursor += n;

        return vec;
    }

    // reads all remaining data in the buffer as-is
    std::vector<uint8_t> read_to_end()
    {
        auto vec = std::vector<uint8_t>(this->at(cursor), this->at(cursor + this->remaining()));
        this->cursor += this->remaining();
        return vec;
    }

    // -- writing
    void write_u8(uint8_t x)
    {
        this->check_space(1);
        this->buffer[cursor] = x;
        cursor++;
    }

    void write_u16(uint16_t x)
    {
        this->check_space(2);
        x = htons(x);
        std::memcpy(this->at(cursor), &x, 2);
        cursor += 2;
    }

    void write_u32(uint32_t x)
    {
        this->check_space(4);
        x = htonl(x);
        std::memcpy(this->at(cursor), &x, 4);
        cursor += 4;
    }

    void write_n(uint8_t* data, size_t n)
    {
        this->check_space(n);
        if (host_is_network_byte_order())
            std::copy(data, data + n, this->at(cursor));
        else
            std::reverse_copy(data, data + n, this->at(cursor));
        this->cursor += n;
    }

    // append some data into the buffer as-is
    // mostly useful for writing the payload unperturbed
    // does not use or adjust the cursor
    void append_raw(uint8_t* data, size_t n) { this->buffer.insert(this->buffer.end(), data, data + n); }
};

#pragma once

#include <optional>
#include <print>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "packed_buffer.h"

struct Ipv4Header {
    uint8_t version;
    uint8_t ihl;
    uint8_t dscp;
    uint8_t ecn;
    uint16_t len;
    uint16_t id;
    uint8_t flags;
    uint16_t fragoff;
    uint8_t ttl;
    uint8_t proto;
    uint16_t chksum;
    uint32_t src;
    uint32_t dst;
    std::vector<uint8_t> options;

    bool mf_flag() { return (this->flags & 1) == 1; }

    static Ipv4Header deserialize(PackedBuffer& pb)
    {
        std::println("!A");

        Ipv4Header that {};

        auto ihl__version = pb.read_u8();
        that.ihl          = ihl__version & 0b00001111;
        that.version      = (ihl__version & 0b11110000) >> 4;

        auto ecn__dscp = pb.read_u8();
        that.ecn       = ecn__dscp & 0b00111111;
        that.dscp      = (ecn__dscp & 0b11000000) >> 6;

        that.len = pb.read_u16();
        that.id  = pb.read_u16();
        std::println("!B");

        auto fragoff__flags = pb.read_u16();
        that.fragoff        = fragoff__flags & 0b00000000'00000111;
        that.flags          = (fragoff__flags & 0b11111111'11111000) >> 3;

        if (that.mf_flag())
            throw std::runtime_error("fragmented");

        std::println("!c; {}", pb.cursor);

        that.ttl = pb.read_u8();
        std::println("!d; {}", pb.cursor);
        that.proto = pb.read_u8();
        std::println("!e; {}", pb.cursor);
        that.chksum = pb.read_u16();
        std::println("!f; {}", pb.cursor);
        that.src = pb.read_u32();
        std::println("!g; {}", pb.cursor);
        that.dst = pb.read_u32();
        std::println("!h; {}", pb.cursor);
        std::println("ihl: {}", that.ihl);

        if (that.ihl > 5)
            // uncertain of the endianness of this field
            // it might have some internal structure that should be considered
            // but we don't really care about it, so just consume the bytes
            that.options = pb.read_n((that.ihl - 5) * 4);

        std::println("ipv4 done, cursor: {}", pb.cursor);

        return that;
    }
};

struct TcpHeader {
    uint16_t sport;
    uint16_t dport;
    uint32_t seq;
    uint32_t ack;
    std::optional<uint8_t> dataofs = std::nullopt;
    uint8_t flags;
    uint16_t window;
    std::optional<uint16_t> chksum = std::nullopt;
    uint16_t urgptr;
    std::vector<uint8_t> options = std::vector<uint8_t>();

    bool cwr_flag() { return (this->flags & 0b10000000) > 0; }
    bool ece_flag() { return (this->flags & 0b01000000) > 0; }
    bool urg_flag() { return (this->flags & 0b00100000) > 0; }
    bool ack_flag() { return (this->flags & 0b00010000) > 0; }
    bool psh_flag() { return (this->flags & 0b00001000) > 0; }
    bool rst_flag() { return (this->flags & 0b00000100) > 0; }
    bool syn_flag() { return (this->flags & 0b00000010) > 0; }
    bool fin_flag() { return (this->flags & 0b00000001) > 0; }

    // calculate the size of the tcp header
    size_t size()
    {
        if (this->dataofs) {
            return *this->dataofs * 4;
        } else {
            if (this->options.empty())
                return 20;

            return 20 + ((this->options.size() - 1) / 4 + 1) * 4;
        }
    }

    static TcpHeader deserialize(PackedBuffer& pb)
    {
        auto offset = pb.cursor;

        TcpHeader that {};

        that.sport   = pb.read_u16();
        that.dport   = pb.read_u16();
        that.seq     = pb.read_u32();
        that.ack     = pb.read_u32();
        that.dataofs = pb.read_u8() >> 4;
        that.flags   = pb.read_u8();
        that.window  = pb.read_u16();
        that.chksum  = pb.read_u16();
        that.urgptr  = pb.read_u16();

        std::println("{} < {}?", pb.cursor, that.size());

        while ((pb.cursor - offset) < that.size()) {
            uint8_t kind = pb.read_u8();
            std::println("kind: {}", kind);
            that.options.push_back(kind);

            // end of options
            if (kind == 0) {
                break;
            }

            // no-op
            if (kind == 1) {
                continue;
            }

            // -2 because the length includes the kind and length fields themselves
            uint8_t length = pb.read_u8() - 2;
            std::println("length: {}; {}; {}", length, pb.cursor, pb.buffer.size());
            that.options.push_back(length);

            auto data = pb.read_n(length);
            that.options.insert(that.options.end(), data.begin(), data.end());
        }

        return that;
    }

    std::vector<uint8_t> serialize(uint32_t src, uint32_t dst, std::vector<uint8_t> payload)
    {
        // the IPv4 pseudoheader is 12 bytes long
        PackedBuffer msg {12 + this->size()};

        // IPv4 pseudoheader
        msg.write_u32(src);
        msg.write_u32(dst);
        msg.write_u8(0);
        msg.write_u8(6);
        std::println("writing size... {}", this->size() + payload.size());
        msg.write_u16(this->size() + payload.size());

        auto tcp_start = msg.cursor;

        // TCP header
        msg.write_u16(this->sport);
        msg.write_u16(this->dport);
        msg.write_u32(this->seq);
        msg.write_u32(this->ack);
        auto dataofs_offset = msg.cursor;
        msg.write_u8(0);
        msg.write_u8(this->flags);
        msg.write_u16(this->window);

        // we will fill in the actual checksum later
        auto checksum_offset = msg.cursor;
        msg.write_u16(0);
        msg.write_u16(this->urgptr);

        for (size_t i = 0; i < this->options.size();) {
            auto kind = this->options[i];
            msg.write_u8(kind);
            i++;

            if (kind == 0) {
                break;
            }

            if (kind == 1) {
                continue;
            }

            auto length = this->options[i];
            msg.write_u8(length);
            i++;

            msg.write_n(this->options.data() + i, length);
            i += length;
        }

        auto padding = (4 - ((msg.cursor - 12) % 4)) % 4;
        for (size_t i = 0; i < padding; i++)
            msg.write_u8(0);

        if (msg.cursor != msg.buffer.size())
            throw std::runtime_error("likely bug: tcp header did not serialize as the expected size");

        // calculate the data offset and write
        // length is now an exact multiple of 4
        auto dataofs = (msg.cursor + 1 - 12) / 4;
        msg.cursor   = dataofs_offset;
        msg.write_u8(dataofs << 4);

        // write payload
        msg.append_raw(payload.data(), payload.size());

        // calculate and write checksum
        auto chksum = msg.ipv4_checksum();
        msg.cursor  = checksum_offset;
        msg.write_u16(chksum);

        // drop the pseudo ipv4 header, but keep the payload
        return std::vector(msg.buffer.begin() + tcp_start, msg.buffer.end());
    }
};

struct TcpMessage {
    std::optional<Ipv4Header> ipv4 = std::nullopt;
    TcpHeader tcp;
    std::vector<uint8_t> payload = {};

    std::vector<uint8_t> serialize(uint32_t src, uint32_t dst) { return tcp.serialize(src, dst, this->payload); }
    static TcpMessage deserialize(PackedBuffer pb)
    {
        auto ipv4 = Ipv4Header::deserialize(pb);
        auto tcp  = TcpHeader::deserialize(pb);
        return TcpMessage {.ipv4 = ipv4, .tcp = tcp, .payload = pb.read_to_end()};
    }
};

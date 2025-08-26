#include <optional>
#include <print>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "packed_buffer.h"

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

    static std::pair<TcpHeader, std::vector<uint8_t>> deserialize(PackedBuffer pb)
    {
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

        while (pb.cursor < that.size()) {
            uint8_t kind = pb.read_u8();

            that.options.push_back(kind);

            // end of options
            if (kind == 0) {
                break;
            }

            // no-op
            if (kind == 1) {
                continue;
            }

            uint8_t length = pb.read_u8();
            that.options.push_back(length);

            auto data = pb.read_n(length);
            that.options.insert(that.options.end(), data.begin(), data.end());
        }

        return std::make_pair(that, pb.read_to_end());
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

        auto padding = 4 - ((msg.cursor + 1) % 4);
        for (size_t i = 0; i < padding; i++)
            msg.write_u8(0);

        if (msg.cursor != msg.buffer.size() - 1)
            throw std::runtime_error("likely bug: tcp header did not serialize as the expected size");

        // calculate the data offset and write
        // length is now an exact multiple of 4
        auto dataofs = (msg.cursor + 1) / 4;
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
    TcpHeader header;
    std::vector<uint8_t> payload;

    std::vector<uint8_t> serialize(uint32_t src, uint32_t dst) { return header.serialize(src, dst, this->payload); }
    static TcpMessage deserialize(PackedBuffer pb)
    {
        auto [header, payload] = TcpHeader::deserialize(pb);
        return TcpMessage {.header = header, .payload = payload};
    }
};

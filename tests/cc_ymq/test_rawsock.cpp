#include <iomanip>
#include <optional>
#include <print>
#include <string>
#include <vector>

#include "tcp.h"

int main()
{
    std::string sMsg = "foobar";
    auto vMsg        = std::vector<uint8_t>(sMsg.begin(), sMsg.end());
    TcpMessage msg {
        .header =
            {.sport   = 2555,
             .dport   = 6789,
             .seq     = 1,
             .ack     = 2,
             .flags   = 0b10101010,
             .window  = 322,
             .urgptr  = 42,
             .options = {2, 4, 0, 0, 233, 0}},
        .payload = vMsg};

    auto data = msg.serialize(192 << 24 | 168 << 16 | 0 << 8 | 1, 127 << 24 | 0 << 16 | 0 << 8 | 1);

    std::println("Serialized TCP header: {} bytes", data.size());
    auto deser = TcpMessage::deserialize(PackedBuffer(data));

    for (auto x: data)
        std::print("\\x{:02x}", x);
    std::println();

    // // print out all the fields of `deser`
    std::println("source_port: {}", deser.header.sport);
    std::println("destination_port: {}", deser.header.dport);
    std::println("seq_num: {}", deser.header.seq);
    std::println("ack_num: {}", deser.header.ack);
    std::println("data_offset__reserved: {}", *deser.header.dataofs);
    std::println("flags: {}", deser.header.flags);
    std::println("window: {}", deser.header.window);
    std::println("checksum: 0x{:x}", *deser.header.chksum);
    std::println("urgent_pointer: {}", deser.header.urgptr);
    std::println("options: {}", deser.header.options.size());
    std::println("message: {}", std::string(deser.payload.begin(), deser.payload.end()));
}

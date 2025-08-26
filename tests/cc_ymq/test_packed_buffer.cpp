#include <cassert>
#include <limits>
#include <print>
#include <vector>

#include "packed_buffer.h"

int main()
{
    PackedBuffer pb(10);
    pb.write_u8(33);
    pb.write_u16(25711);
    pb.write_u32(9999999);
    uint8_t data[] = {7,9,11};
    pb.write_n(data, 3);

    std::println("{}", pb.buffer);

    pb.reset();
    auto a = pb.read_u8();
    auto b = pb.read_u16();
    auto c = pb.read_u32();
    auto d = pb.read_n(3);

    std::println("{}; {}; {}; {}", a, b, c, d);

    // assert((pb.buffer == std::vector<uint8_t> {0, 1, 2, 3}));
}

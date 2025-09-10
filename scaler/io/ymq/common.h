#pragma once

// C
#ifdef __linux__
#include <execinfo.h>
#endif  // __linux__

// C++
#include <cstdlib>
#include <cstring>
#include <format>
#include <iostream>
#include <source_location>
#include <string>

using Errno = int;

inline void print_trace(void)
{
#ifdef __linux__
    void* array[10];
    char** strings;
    int size, i;

    size    = backtrace(array, 10);
    strings = backtrace_symbols(array, size);
    if (strings != NULL) {
        printf("Obtained %d stack frames.\n", size);
        for (i = 0; i < size; i++)
            printf("%s\n", strings[i]);
    }

    free(strings);
#endif  // __linux__
}

[[nodiscard("Memory is allocated but not used, likely causing a memory leak")]]
inline uint8_t* datadup(const uint8_t* data, size_t len) noexcept
{
    uint8_t* dup = new uint8_t[len];  // we just assume allocation will succeed
    std::memcpy(dup, data, len);
    return dup;
}

inline void serialize_u32(uint32_t x, uint8_t buffer[4])
{
    buffer[0] = x & 0xFF;
    buffer[1] = (x >> 8) & 0xFF;
    buffer[2] = (x >> 16) & 0xFF;
    buffer[3] = (x >> 24) & 0xFF;
}

inline void deserialize_u32(const uint8_t buffer[4], uint32_t* x)
{
    *x = buffer[0] | buffer[1] << 8 | buffer[2] << 16 | buffer[3] << 24;
}

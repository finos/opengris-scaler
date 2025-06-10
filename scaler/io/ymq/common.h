#pragma once

// C
#include <execinfo.h>

// C++
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <source_location>
#include <string>

const size_t HEADER_SIZE = 4;  // size of the message header in bytes

using Errno = int;

inline void print_trace(void) {
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
}

// this is an unrecoverable error that exits the program
// prints a message plus the source location
[[noreturn]] inline void panic(
    std::string message, const std::source_location& location = std::source_location::current()) {
    auto file_name = std::string(location.file_name());
    file_name      = file_name.substr(file_name.find_last_of("/") + 1);

    std::cout << "panic at " << file_name << ":" << location.line() << ":" << location.column() << " in function ["
              << location.function_name() << "]: " << message << std::endl;

    print_trace();

    std::abort();
}

[[noreturn]] inline void todo(
    std::optional<std::string> message   = std::nullopt,
    const std::source_location& location = std::source_location::current()) {
    if (message) {
        panic("TODO: " + *message, location);
    } else {
        panic("TODO", location);
    }
}

inline uint8_t* datadup(const uint8_t* data, size_t len) {
    uint8_t* dup = new uint8_t[len];
    std::memcpy(dup, data, len);
    return dup;
}

inline void serialize_u32(uint32_t x, uint8_t buffer[4]) {
    buffer[0] = x & 0xFF;
    buffer[1] = (x >> 8) & 0xFF;
    buffer[2] = (x >> 16) & 0xFF;
    buffer[3] = (x >> 24) & 0xFF;
}

inline void deserialize_u32(const uint8_t buffer[4], uint32_t* x) {
    *x = buffer[0] | buffer[1] << 8 | buffer[2] << 16 | buffer[3] << 24;
}

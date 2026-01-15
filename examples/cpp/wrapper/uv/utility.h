
#include <expected>
#include <iostream>
#include <type_traits>

#include "scaler/wrapper/uv/error.h"

// Simple helper that exits the program when it receives a std::unexpected value.
template <typename T>
T exitOnFailure(std::expected<T, scaler::wrapper::uv::Error> result)
{
    if (!result.has_value()) {
        scaler::wrapper::uv::Error error = result.error();
        std::cerr << "Operation failed: " << error.message() << " (" << error.name() << ")\n";
        std::exit(1);
    }
    if constexpr (!std::is_void_v<T>) {
        return std::move(result.value());
    }
}

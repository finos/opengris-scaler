
#include <expected>
#include <type_traits>

#include "scaler/wrapper/uv/error.h"

// Extract the value from std::expected or fail the test
template <typename T>
static T expectSuccess(std::expected<T, scaler::wrapper::uv::Error> result)
{
    if (!result.has_value()) {
        scaler::wrapper::uv::Error error = result.error();
        throw std::runtime_error("Operation failed: " + error.message() + " (" + error.name() + ")");
    }

    if constexpr (!std::is_void_v<T>) {
        return std::move(result.value());
    }
}

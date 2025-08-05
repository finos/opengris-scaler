#pragma once

#include <cstddef>
#include <format>
#include <print>
#include <string>

#include "scaler/io/ymq/timestamp.h"
#include "scaler/io/ymq/utils.h"

namespace scaler {
namespace ymq {

enum LoggingLevel {
    debug,
    info,
    error,
};

// Sound default logging level based on build type.
#ifdef NDEBUG  // Release build
inline LoggingLevel LOGGING_LEVEL = LoggingLevel::info;
#else  // Debug build
inline LoggingLevel LOGGING_LEVEL = LoggingLevel::debug;
#endif

constexpr std::string_view convertLevelToString(LoggingLevel level)
{
    switch (level) {
        case debug: return "[DEBUG]";
        case info: return "[INFO]";
        case error: return "[ERROR]";
    }
    return "[UNKNOWN]";
}

template <typename... Args>
constexpr void log(LoggingLevel level, Args&&... args)
{
    if (level < LOGGING_LEVEL)
        return;
    auto str = argsToString(convertLevelToString(level), Timestamp {}, std::forward<Args>(args)...);
    std::print("{}\n", str);
}

}  // namespace ymq
}  // namespace scaler

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
    critical,
    error,
    warning,
    info,
    debug,
    notset
};

// Sound default logging level based on build type.
#ifdef NDEBUG  // Release build
inline LoggingLevel LOGGING_LEVEL = LoggingLevel::info;
#else  // Debug build
inline LoggingLevel LOGGING_LEVEL = LoggingLevel::debug;
#endif

inline LoggingLevel stringToLogLevel(std::string_view level_sv)
{
    if (level_sv == "CRITICAL") return LoggingLevel::critical;
    if (level_sv == "ERROR") return LoggingLevel::error;
    if (level_sv == "WARNING") return LoggingLevel::warning;
    if (level_sv == "INFO") return LoggingLevel::info;
    if (level_sv == "DEBUG") return LoggingLevel::debug;
    return LoggingLevel::info; // Default
}

constexpr std::string_view convertLevelToString(LoggingLevel level)
{
    switch (level) {
        case debug: return "DEBG";
        case info: return "INFO";
        case error: return "EROR";
        case warning: return "WARN";
        case critical: return "CTIC";
        case notset: return "NOTSET";
    }
    return "UNKNOWN";
}

inline void replaceAll(std::string& str, const std::string& from, const std::string& to) {
    if (from.empty())
        return;
    size_t start_pos = 0;
    while ((start_pos = str.find(from, start_pos)) != std::string::npos) {
        str.replace(start_pos, from.length(), to);
        start_pos += to.length();
    }
}

template <typename... Args>
void log(LoggingLevel level, const std::string& format, Args&&... args)
{
    if (level < LOGGING_LEVEL)
        return;

    std::ostringstream message_stream;
    // Use if constexpr to handle the variadic arguments correctly
    if constexpr (sizeof...(args) > 0) {
        (message_stream << ... << args);
    }
    std::string message = message_stream.str();

    std::ostringstream ts_stream;
    ts_stream << Timestamp{};
    std::string timestamp_str = ts_stream.str();

    std::string output = format;
    replaceAll(output, "%(levelname)s", std::string(convertLevelToString(level)));
    replaceAll(output, "%(asctime)s", timestamp_str); 
    replaceAll(output, "%(message)s", message);
    
    replaceAll(output, "%(name)s", "cpp-logger");
    replaceAll(output, "%(lineno)d", "0");

    std::cout << output << std::endl;
}

}  // namespace ymq
}  // namespace scaler

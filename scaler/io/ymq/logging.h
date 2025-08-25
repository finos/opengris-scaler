#pragma once

#include <concepts> 
#include <cstddef>
#include <format>
#include <fstream>
#include <print>
#include <string>
#include <type_traits>

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

template <typename T1, typename T2, typename... Args>
    requires(std::is_convertible_v<T1, std::string_view> && std::is_convertible_v<T2, std::string_view>)
void log(LoggingLevel level, T1&& generic_format, T2&& generic_log_path, Args&&... args)
{
    if (level < LOGGING_LEVEL)
        return;

    std::string format = std::forward<T1>(generic_format);
    std::string log_path = std::forward<T2>(generic_log_path);

    std::ostringstream output_stream;
    size_t last_pos = 0;
    size_t find_pos = 0;

    while ((find_pos = format.find('%', last_pos)) != std::string::npos) {
        // Write the literal text since the last placeholder
        output_stream.write(format.c_str() + last_pos, find_pos - last_pos);

        if (format.length() > find_pos + 1 && format[find_pos + 1] == '%') {
            output_stream.put('%');
            last_pos = find_pos + 2;
            continue;
        }

        if (format.length() > find_pos + 1 && format[find_pos + 1] == '(') {
            size_t token_end = format.find(")s", find_pos + 2);
            if (token_end != std::string::npos) {
                std::string_view token = std::string_view(format).substr(find_pos + 2, token_end - (find_pos + 2));
                
                if (token == "levelname") {
                    output_stream << convertLevelToString(level);
                } else if (token == "asctime") {
                    output_stream << Timestamp{};
                } else if (token == "message") {
                    // Stream the message arguments directly
                    if constexpr (sizeof...(args) > 0) {
                        (output_stream << ... << std::forward<Args>(args));
                    }
                } else if (token == "name") {
                    output_stream << "cpp-logger";
                } else if (token == "lineno") {
                    output_stream << "0";
                } else {
                    // Unknown token, print literally
                    output_stream.write(format.c_str() + find_pos, token_end + 2 - find_pos);
                }
                last_pos = token_end + 2;
                continue;
            }
        }
        
        // Not a valid token, print the '%' literally
        output_stream.put('%');
        last_pos = find_pos + 1;
    }

    // Write any remaining literal text after the last placeholder
    if (last_pos < format.length()) {
        output_stream.write(format.c_str() + last_pos, format.length() - last_pos);
    }

    std::string formatted_message = output_stream.str();

    if (log_path.empty() || log_path == "/dev/stdout") {
        // Use std::print and flush for immediate output to the terminal
        std::print("{}\n", formatted_message);
        std::fflush(stdout);
    } else {
        // Open the file in append mode and write the log message
        std::ofstream log_file(log_path, std::ios_base::app);
        if (log_file.is_open()) {
            log_file << formatted_message << std::endl;
        } else {
            // Fallback to printing to standard error if the file cannot be opened
            std::cerr << "Error: Could not open log file: " << log_path << std::endl;
            std::cerr << formatted_message << std::endl;
        }
    }
}

template <typename... Args>
    requires(
        sizeof...(Args) < 2 || // Always enable for 0 or 1 argument...
        // ...OR if there are 2+ arguments and they DON'T match the signature of Overload 1.
        !(std::is_convertible_v<std::tuple_element_t<0, std::tuple<Args...>>, std::string_view> &&
          std::is_convertible_v<std::tuple_element_t<1, std::tuple<Args...>>, std::string_view>)
    )
void log(LoggingLevel level, Args&&... args) {
    log(level, 
        std::string("%(levelname)s: %(message)s"), 
        std::string("/dev/stdout"), 
        std::forward<Args>(args)...);
}

}  // namespace ymq
}  // namespace scaler

#pragma once

#include <uv.h>

#include <cctype>
#include <cstddef>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#include "scaler/ymq/timestamp.h"
#include "scaler/ymq/utils.h"

namespace scaler {
namespace ymq {

class Logger {
public:
    enum LoggingLevel { critical = 0, error = 1, warning = 2, info = 3, debug = 4, notset = 5 };

// Sound default logging level based on build type.
#ifdef NDEBUG  // Release build
    static constexpr LoggingLevel DEFAULT_LOGGING_LEVEL = LoggingLevel::info;
#else  // Debug build
    static constexpr LoggingLevel DEFAULT_LOGGING_LEVEL = LoggingLevel::debug;
#endif

    Logger(
        std::string log_format             = "%(asctime)s %(levelname)s %(name)s[%(process)d]: %(message)s",
        std::vector<std::string> log_paths = {"/dev/stdout"},
        LoggingLevel level                 = DEFAULT_LOGGING_LEVEL,
        std::string name                   = "scaler")
        : _log_paths(std::move(log_paths)), _level(level), _name(std::move(name))
    {
        preprocessFormat(log_format);
    }

    static LoggingLevel stringToLogLevel(std::string_view level_sv)
    {
        if (level_sv == "CRITICAL")
            return LoggingLevel::critical;
        if (level_sv == "ERROR")
            return LoggingLevel::error;
        if (level_sv == "WARNING")
            return LoggingLevel::warning;
        if (level_sv == "INFO")
            return LoggingLevel::info;
        if (level_sv == "DEBUG")
            return LoggingLevel::debug;
        return LoggingLevel::info;  // Default
    }

    static constexpr std::string_view convertLevelToString(LoggingLevel level)
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

    template <typename... Args>
    void log(LoggingLevel level, Args&&... args) const
    {
        if (level > _level) {
            return;
        }

        std::ostringstream output_stream;
        for (const auto& part: _processed_format) {
            if (part.is_token) {
                if (part.content == "levelname") {
                    output_stream << convertLevelToString(level);
                } else if (part.content == "asctime") {
                    output_stream << Timestamp {};
                } else if (part.content == "message") {
                    if constexpr (sizeof...(args) > 0) {
                        (output_stream << ... << std::forward<Args>(args));
                    }
                } else if (part.content == "name") {
                    output_stream << _name;
                } else if (part.content == "process") {
                    uv_pid_t pid = uv_os_getpid();
                    output_stream << pid;
                } else if (part.content == "lineno") {
                    output_stream << "0";
                } else {
                    // Unknown token, print literally (with the original specifier)
                    output_stream << "%(" << part.content << ")" << part.spec;
                }
            } else {
                output_stream << part.content;
            }
        }

        std::string formatted_message = output_stream.str();

        for (auto log_path: _log_paths) {
            if (log_path.empty() || log_path == "/dev/stdout") {
                std::cout << formatted_message << std::endl;
                std::fflush(stdout);
            } else {
                // Open the file in append mode and write the log message
                std::ofstream log_file(log_path, std::ios_base::app);
                if (log_file.is_open()) {
                    log_file << formatted_message << std::endl;
                } else {
                    throw std::runtime_error("Error: Could not open log file: " + log_path + '\n' + formatted_message);
                }
            }
        }
    }

private:
    struct FormatPart {
        std::string content;
        bool is_token;
        // For tokens, the format specifier letter that followed ')' (e.g. 's' in "%(name)s",
        // 'd' in "%(process)d"). Kept so that unknown tokens can be emitted verbatim.
        char spec = '\0';
    };

    std::vector<FormatPart> _processed_format;
    std::vector<std::string> _log_paths;
    LoggingLevel _level;
    std::string _name;

    void preprocessFormat(const std::string& format)
    {
        size_t last_pos = 0;
        size_t find_pos = 0;

        while ((find_pos = format.find('%', last_pos)) != std::string::npos) {
            if (find_pos > last_pos) {
                _processed_format.push_back({format.substr(last_pos, find_pos - last_pos), false});
            }

            if (format.length() > find_pos + 1 && format[find_pos + 1] == '%') {
                _processed_format.push_back({"%", false});
                last_pos = find_pos + 2;
                continue;
            }

            if (format.length() > find_pos + 1 && format[find_pos + 1] == '(') {
                size_t close_paren = format.find(')', find_pos + 2);
                if (close_paren != std::string::npos && close_paren + 1 < format.length() &&
                    std::isalpha(static_cast<unsigned char>(format[close_paren + 1]))) {
                    std::string token = format.substr(find_pos + 2, close_paren - (find_pos + 2));
                    _processed_format.push_back({token, true, format[close_paren + 1]});
                    last_pos = close_paren + 2;
                    continue;
                }
            }
            _processed_format.push_back({"%", false});
            last_pos = find_pos + 1;
        }

        if (last_pos < format.length()) {
            _processed_format.push_back({format.substr(last_pos), false});
        }
    }
};

}  // namespace ymq
}  // namespace scaler

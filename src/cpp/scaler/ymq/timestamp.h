#pragma once

#include <cassert>
#include <chrono>
#include <format>
#include <iomanip>
#include <ostream>
#include <sstream>  // stringify

namespace scaler {
namespace ymq {

// Simple timestamp utility
struct Timestamp {
    std::chrono::time_point<std::chrono::system_clock> timestamp;

    friend std::strong_ordering operator<=>(Timestamp x, Timestamp y) { return x.timestamp <=> y.timestamp; }

    Timestamp(): timestamp(std::chrono::system_clock::now()) {}
    Timestamp(std::chrono::time_point<std::chrono::system_clock> t) { timestamp = std::move(t); }

    template <typename Rep, typename Period = std::ratio<1>>
    Timestamp operator+(std::chrono::duration<Rep, Period> offset) const
    {
        return {timestamp + offset};
    }

    template <typename Rep, typename Period = std::ratio<1>>
    Timestamp operator-(std::chrono::duration<Rep, Period> offset) const
    {
        return {timestamp - offset};
    }

    friend std::chrono::milliseconds operator-(Timestamp lhs, Timestamp rhs)
    {
        return std::chrono::duration_cast<std::chrono::milliseconds>(lhs.timestamp - rhs.timestamp);
    }
};

inline std::string stringifyTimestamp(Timestamp ts)
{
    const auto ts_seconds {std::chrono::floor<std::chrono::seconds>(ts.timestamp)};
    const std::time_t system_time = std::chrono::system_clock::to_time_t(ts_seconds);
    const std::tm local_time      = *std::localtime(&system_time);

    std::ostringstream oss;
    oss << std::put_time(&local_time, "%F %T%z");
    return oss.str();
}

inline std::ostream& operator<<(std::ostream& os, const Timestamp& ts)
{
    os << stringifyTimestamp(ts);
    return os;
}

}  // namespace ymq
}  // namespace scaler

template <>
struct std::formatter<scaler::ymq::Timestamp, char> {
    template <typename ParseContext>
    constexpr ParseContext::iterator parse(ParseContext& ctx)
    {
        return ctx.begin();
    }

    template <typename FmtContext>
    constexpr FmtContext::iterator format(scaler::ymq::Timestamp ts, FmtContext& ctx) const
    {
        return std::format_to(ctx.out(), "{}", scaler::ymq::stringifyTimestamp(ts));
    }
};

#pragma once

#include <algorithm>
#include <array>
#include <cstring>
#include <format>
#include <string>

namespace scaler {
namespace ymq {

template <std::size_t N>
    requires(N > 0)
consteval auto getFormatString()
{
    std::string str = "{}";
    for (size_t i = 1; i < N; ++i)
        str += ": {}";
    std::array<char, (N - 1) * 4 + 2> arr;
    std::ranges::copy(str, arr.begin());
    return arr;
}

// NOTE: It seems like this two lines cannot be placed in the constructor for unknown reason.
template <typename... Args>
constexpr std::string argsToString(Args&&... args)
{
    static constexpr const auto str = getFormatString<sizeof...(Args)>();

    std::string res = std::format(std::string_view {str}, std::forward<Args>(args)...);
    return res;
}

}  // namespace ymq
}  // namespace scaler

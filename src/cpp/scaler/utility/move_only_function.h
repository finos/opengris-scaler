#pragma once

#include <concepts>
#include <functional>
#include <memory>
#include <type_traits>

namespace scaler {
namespace utility {

// Use feature-test macro to detect support for std::move_only_function.
// This works across GCC, Clang, and MSVC on all platforms.
// Otherwise, we provide a basic implementation.
#if defined(__cpp_lib_move_only_function) && __cpp_lib_move_only_function >= 202110L
template <typename T>
using MoveOnlyFunction = std::move_only_function<T>;
#else

template <typename...>
class MoveOnlyFunction;

template <typename R, typename... Args>
class MoveOnlyFunction<R(Args...)> {
public:
    MoveOnlyFunction() = default;

    MoveOnlyFunction(MoveOnlyFunction&&) noexcept = default;

    MoveOnlyFunction& operator=(MoveOnlyFunction&&) noexcept = default;

    template <typename F>
    MoveOnlyFunction(F&& f): callable_(std::make_unique<CallableContainer<F>>(std::forward<F>(f)))
    {
    }

    MoveOnlyFunction(const MoveOnlyFunction&)            = delete;
    MoveOnlyFunction& operator=(const MoveOnlyFunction&) = delete;

    R operator()(Args... args) { return (*callable_)(std::forward<Args>(args)...); }

    explicit operator bool() const noexcept { return static_cast<bool>(callable_); }

private:
    // Required for type-erasure, so that we support std::function, lambdas, function pointers ...
    struct CallableBase {
        virtual ~CallableBase()                  = default;
        virtual R operator()(Args... args) const = 0;
    };

    template <typename F>
        requires std::invocable<F, Args...> && std::convertible_to<std::invoke_result_t<F, Args...>, R>
    struct CallableContainer: CallableBase {
        mutable F f;
        explicit CallableContainer(F&& f_): f(std::forward<F>(f_)) {}
        R operator()(Args... args) const override { return std::invoke(f, std::forward<Args>(args)...); }
    };

    std::unique_ptr<CallableBase> callable_;
};

#endif

}  // namespace utility
}  // namespace scaler

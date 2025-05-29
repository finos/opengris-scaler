#pragma once

#include <coroutine>
#include <exception>

// Do not change these names. They are very special names
struct task {
    struct promise_type {
        std::exception_ptr excp;
        // Take notice here!
        auto initial_suspend() noexcept { return std::suspend_never {}; }
        auto final_suspend() noexcept { return std::suspend_always {}; }
        task get_return_object() { return {handle_t::from_promise(*this)}; }
        void unhandled_exception() { excp = std::current_exception(); }
    };

    using handle_t = std::coroutine_handle<promise_type>;
    handle_t handle;
};

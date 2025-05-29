#pragma once
#include <coroutine>

// TODO: This aclass is just dummy, change later.
struct aclass {
    virtual int complete() = 0;
};

struct awaitable {
    aclass* context;

    bool await_ready() { return false; }

    void await_suspend(std::coroutine_handle<> handle) noexcept {
        // context->handle = handle;
    }

    int await_resume() { return context->complete(); }
};

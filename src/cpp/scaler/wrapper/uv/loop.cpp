#include "scaler/wrapper/uv/loop.h"

#include <cassert>

#include "scaler/wrapper/uv/callback.h"
#include "scaler/wrapper/uv/handle.h"

namespace scaler {
namespace wrapper {
namespace uv {

// A graceful shutdown keeps a stream open until its writes drain, which a stalled peer can prevent forever.
static void closeLingeringStream(uv_handle_t* handle, [[maybe_unused]] void* arg) noexcept
{
    if (uv_is_closing(handle)) {
        return;
    }

    switch (uv_handle_get_type(handle)) {
        case UV_TCP: uv_close(handle, &Handle<uv_tcp_t, ReadCallback>::free); break;
        case UV_NAMED_PIPE: uv_close(handle, &Handle<uv_pipe_t, ReadCallback>::free); break;
        default: break;
    }
}

std::expected<Loop, Error> Loop::init(std::initializer_list<LoopOption> options) noexcept
{
    Loop loop {};

    // Initialize the loop
    int err = uv_loop_init(&loop.native());
    if (err) {
        return std::unexpected {Error {err}};
    }

    // Configure loop options if provided
    for (const auto& option: options) {
        if (option._argument.has_value()) {
            // Option with argument (e.g., UV_LOOP_BLOCK_SIGNAL)
            err = uv_loop_configure(&loop.native(), option._option, option._argument.value());
        } else {
            // Option without argument
            err = uv_loop_configure(&loop.native(), option._option);
        }

        if (err) {
            return std::unexpected {Error {err}};
        }
    }

    return loop;
}

int Loop::run(uv_run_mode mode) noexcept
{
    return uv_run(&native(), mode);
}

void Loop::stop() noexcept
{
    uv_stop(&native());
}

void Loop::loopDeleter(uv_loop_t* loop) noexcept
{
    uv_walk(loop, &closeLingeringStream, nullptr);

    if (uv_loop_alive(loop)) {
        // Run a final iteration of the loop until all `uv_close()` callbacks are called.
        [[maybe_unused]] int nActiveHandles = uv_run(loop, UV_RUN_DEFAULT);
        assert(nActiveHandles == 0 && "Loop is still alive");
    }

    [[maybe_unused]] const int err = uv_loop_close(loop);
    assert(!err && "uv_loop_close failed");

    delete loop;
}

}  // namespace uv
}  // namespace wrapper
}  // namespace scaler

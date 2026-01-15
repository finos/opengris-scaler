#include "scaler/wrapper/uv/loop.h"

#include <cassert>

namespace scaler {
namespace wrapper {
namespace uv {

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
    if (uv_loop_alive(loop)) {
        // Run a non-blocking final iteration.
        // That's because some handles might still have pending calls to `uv_close()` that were triggered by RAII.
        int nActiveHandles = uv_run(loop, UV_RUN_NOWAIT);
        assert(nActiveHandles == 0 && "Loop is still alive");
    }

    const int err = uv_loop_close(loop);
    assert(!err && "uv_loop_close failed");

    delete loop;
}

}  // namespace uv
}  // namespace wrapper
}  // namespace scaler

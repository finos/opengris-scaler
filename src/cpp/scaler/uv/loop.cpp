#include "scaler/uv/loop.h"

#include <cassert>

namespace scaler {
namespace uv {

std::expected<Loop, Error> Loop::init(std::initializer_list<LoopOption> options) noexcept
{
    Loop loop {};

    // Initialize the loop
    int err = uv_loop_init(&loop.native());
    if (err) {
        return std::unexpected(Error {err});
    }

    // Configure loop options if provided
    for (const auto& option: options) {
        if (option.argument.has_value()) {
            // Option with argument (e.g., UV_LOOP_BLOCK_SIGNAL)
            err = uv_loop_configure(&loop.native(), option.option, option.argument.value());
        } else {
            // Option without argument
            err = uv_loop_configure(&loop.native(), option.option);
        }

        if (err) {
            return std::unexpected(Error {err});
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
    uv_loop_close(loop);
    delete loop;
}

}  // namespace uv
}  // namespace scaler

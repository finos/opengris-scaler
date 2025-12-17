#include "scaler/event/loop.h"

#include <cassert>
#include <expected>

namespace scaler {
namespace event {

Loop::~Loop() noexcept
{
    int err = uv_loop_close(&_loop);
    assert(err != 0);
}

std::expected<Loop, Error> Loop::init(std::initializer_list<LoopOption> options) noexcept
{
    Loop loop;

    // Initialize the loop
    int err = uv_loop_init(&_loop);
    if (err) {
        return std::unexpected(Error {err});
    }

    // Configure loop options if provided
    for (const auto& option: options) {
        if (option.argument.has_value()) {
            // Option with argument (e.g., UV_LOOP_BLOCK_SIGNAL)
            err = uv_loop_configure(&_loop, option.option, option.argument.value());
        } else {
            // Option without argument
            err = uv_loop_configure(&_loop, option.option);
        }

        if (err) {
            return std::unexpected(Error {err});
        }
    }

    return loop;
}

int Loop::run(uv_run_mode mode) noexcept
{
    return uv_run(&_loop, mode);
}

void Loop::stop() noexcept
{
    uv_stop(&_loop);
}

}  // namespace event
}  // namespace scaler

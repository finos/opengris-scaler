#pragma once

#include <uv.h>

#include <expected>
#include <initializer_list>
#include <optional>

#include "scaler/event/error.h"

namespace scaler {
namespace event {

// See uv_loop_t
class Loop {
public:
    struct LoopOption {
        uv_loop_option option;
        std::optional<int> argument;  // Some options have arguments, e.g. UV_LOOP_BLOCK_SIGNAL
    };

    // See uv_loop_close
    ~Loop() noexcept;

    Loop(const Loop&)            = delete;
    Loop& operator=(const Loop&) = delete;

    Loop(Loop&& other) noexcept            = default;
    Loop& operator=(Loop&& other) noexcept = default;

    // See uv_loop_init, uv_loop_configure
    std::expected<Loop, Error> init(std::initializer_list<LoopOption> options = {}) noexcept;

    // See uv_run
    int run(uv_run_mode mode = UV_RUN_DEFAULT) noexcept;

    // See uv_stop
    void stop() noexcept;

private:
    uv_loop_t _loop;

    Loop() noexcept = default;
};

}  // namespace event
}  // namespace scaler

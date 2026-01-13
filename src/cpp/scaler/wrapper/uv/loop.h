#pragma once

#include <uv.h>

#include <expected>
#include <initializer_list>
#include <memory>
#include <optional>

#include "scaler/wrapper/uv/error.h"

namespace scaler {
namespace wrapper {
namespace uv {

// See uv_loop_t
class Loop {
public:
    struct LoopOption {
        const uv_loop_option _option;
        const std::optional<int> _argument;  // Some options have arguments, e.g. UV_LOOP_BLOCK_SIGNAL
    };

    // See uv_loop_close
    ~Loop() noexcept = default;

    Loop(const Loop&)            = delete;
    Loop& operator=(const Loop&) = delete;

    Loop(Loop&& other) noexcept            = default;
    Loop& operator=(Loop&& other) noexcept = default;

    // See uv_loop_init, uv_loop_configure
    static std::expected<Loop, Error> init(std::initializer_list<LoopOption> options = {}) noexcept;

    constexpr uv_loop_t& native() noexcept { return *_native; };

    constexpr const uv_loop_t& native() const noexcept { return *_native; };

    // See uv_run
    int run(uv_run_mode mode = UV_RUN_DEFAULT) noexcept;

    // See uv_stop
    void stop() noexcept;

private:
    static void loopDeleter(uv_loop_t* loop) noexcept;

    // The uv_loop_t is not movable and has to be heap-allocated, as libuv might hold internal references to it.
    std::unique_ptr<uv_loop_t, decltype(&loopDeleter)> _native {new uv_loop_t(), &loopDeleter};

    Loop() noexcept = default;
};

}  // namespace uv
}  // namespace wrapper
}  // namespace scaler

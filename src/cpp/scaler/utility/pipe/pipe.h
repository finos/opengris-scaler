#pragma once

#include <cstdint>

#include "scaler/utility/pipe/pipe_reader.h"
#include "scaler/utility/pipe/pipe_utils.h"
#include "scaler/utility/pipe/pipe_writer.h"

namespace scaler {
namespace utility {
namespace pipe {

struct Pipe {
public:
    Pipe(): reader(-1), writer(-1)
    {
        std::pair<int64_t, int64_t> pair = createPipe();
        this->reader                     = PipeReader(pair.first);
        this->writer                     = PipeWriter(pair.second);
    }

    ~Pipe() = default;

    // Move-only
    Pipe(Pipe&& other) noexcept: reader(-1), writer(-1)
    {
        this->reader = std::move(other.reader);
        this->writer = std::move(other.writer);
    }

    Pipe& operator=(Pipe&& other) noexcept
    {
        this->reader = std::move(other.reader);
        this->writer = std::move(other.writer);
        return *this;
    }

    Pipe(const Pipe&)            = delete;
    Pipe& operator=(const Pipe&) = delete;

    PipeReader reader;
    PipeWriter writer;
};

}  // namespace pipe
}  // namespace utility
}  // namespace scaler

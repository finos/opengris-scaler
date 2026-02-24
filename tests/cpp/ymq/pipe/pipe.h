#pragma once

#include "tests/cpp/ymq/pipe/pipe_reader.h"
#include "tests/cpp/ymq/pipe/pipe_utils.h"
#include "tests/cpp/ymq/pipe/pipe_writer.h"

struct Pipe {
public:
    Pipe(): reader(-1), writer(-1)
    {
        std::pair<long long, long long> pair = createPipe();
        this->reader                         = PipeReader(pair.first);
        this->writer                         = PipeWriter(pair.second);
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

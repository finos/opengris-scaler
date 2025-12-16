#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <span>
#include <vector>

#include "scaler/utility/io_helpers.h"

using namespace scaler::utility;

class IOHelpersTest: public ::testing::Test {
protected:
    // Simulates the behavior of a reader on a file-like structure of size `inputSize`.
    static std::function<IOResult(const std::span<uint8_t>&)> getMockedReader(size_t inputSize, size_t chunkSize)
    {
        struct ReaderState {
            std::vector<uint8_t> source;
            size_t offset;
        };

        // Shared state between calls to the reader function.
        auto state = std::make_shared<ReaderState>(std::vector<uint8_t>(inputSize, '\0'), 0);

        return [=](const std::span<uint8_t>& buffer) -> IOResult {
            const size_t remaining = state->source.size() - state->offset;

            if (remaining <= 0) {
                return IOResult::failure(IOResult::Error::EndOfFile, 0);
            }

            const size_t toCopy = std::min({remaining, buffer.size(), chunkSize});
            state->offset += toCopy;

            std::copy_n(state->source.cbegin(), toCopy, buffer.begin());

            return IOResult::success(toCopy);
        };
    }

    // Simulates the behavior of a writer on a buffered file-like structure, which trigger a WouldBlock error after
    // `outputSize` bytes written.
    static std::function<IOResult(const std::vector<std::span<const uint8_t>>&)> getMockedWriter(
        size_t outputSize, size_t chunkSize)
    {
        struct WriterState {
            size_t offset;
        };

        // Shared state between calls to the writer function.
        auto state = std::make_shared<WriterState>(0);

        return [=](const std::vector<std::span<const uint8_t>>& buffers) -> IOResult {
            const size_t remaining = outputSize - state->offset;

            if (remaining <= 0) {
                return IOResult::failure(IOResult::Error::WouldBlock, 0);
            }

            size_t totalSize = 0;
            for (const auto& buffer: buffers) {
                totalSize += buffer.size();
            }

            const size_t toWrite = std::min({remaining, totalSize, chunkSize});
            state->offset += toWrite;

            return IOResult::success(toWrite);
        };
    }
};

TEST_F(IOHelpersTest, ReadExact)
{
    const size_t inputSize = 10;
    const size_t chunkSize = 2;
    auto reader            = getMockedReader(inputSize, chunkSize);

    const size_t bufferSize = 6;
    std::vector<uint8_t> buffer(bufferSize);

    // Read first bytes with success
    {
        IOResult res = readExact(buffer, reader);
        EXPECT_FALSE(res.error);
        EXPECT_EQ(res.bytesTransferred, buffer.size());
    }

    // Partially read the first remaining bytes, then EOF
    {
        IOResult res = readExact(buffer, reader);
        EXPECT_EQ(res.error, IOResult::Error::EndOfFile);
        EXPECT_EQ(res.bytesTransferred, inputSize - buffer.size());
    }

    // Subsequent reads fail with EOF
    {
        IOResult res = readExact(buffer, reader);
        EXPECT_EQ(res.error, IOResult::Error::EndOfFile);
        EXPECT_EQ(res.bytesTransferred, 0);
    }
}

TEST_F(IOHelpersTest, WriteAll)
{
    const size_t outputSize = 10;
    const size_t chunkSize  = 2;
    auto writer             = getMockedWriter(outputSize, chunkSize);

    const size_t bufferSize = 2;
    const size_t nBuffers   = 3;
    std::vector<uint8_t> buffer(bufferSize, '\0');
    std::vector<std::span<const uint8_t>> buffers(nBuffers, buffer);

    // Write first buffers with success
    {
        IOResult res = writeAll(buffers, writer);
        EXPECT_FALSE(res.error);
        EXPECT_EQ(res.bytesTransferred, bufferSize * nBuffers);
    }

    // Partially write and receive a WouldBlock error
    {
        IOResult res = writeAll(buffers, writer);
        EXPECT_EQ(res.error, IOResult::Error::WouldBlock);
        EXPECT_EQ(res.bytesTransferred, outputSize - (bufferSize * nBuffers));
    }
}

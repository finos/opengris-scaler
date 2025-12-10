#include <gtest/gtest.h>

#include <array>
#include <span>
#include <string>

#include "scaler/utility/io_result.h"
#include "scaler/utility/pipe/pipe.h"
#include "scaler/utility/pipe/pipe_writer.h"

using namespace scaler::utility::pipe;
using namespace scaler::utility;

class PipeTest: public ::testing::Test {};

TEST_F(PipeTest, Blocking)
{
    Pipe pipe;

    constexpr std::array<uint8_t, 5> message = {'H', 'e', 'l', 'l', 'o'};

    // Write to the pipe
    {
        IOResult result = pipe.writer.writeAll({message});
        ASSERT_FALSE(result.error);
        ASSERT_EQ(result.bytesTransferred, message.size());
    }

    // Read from the pipe
    {
        std::array<uint8_t, message.size()> buffer;
        IOResult result = pipe.reader.readExact(buffer);

        ASSERT_FALSE(result.error);
        ASSERT_EQ(result.bytesTransferred, message.size());

        ASSERT_EQ(buffer, message);
    }

    // Closing the pipe writer triggers an EOF error
    {
        // Move and destruct the writer.
        {
            PipeWriter writer = std::move(pipe.writer);
        }

        std::array<uint8_t, 1> buffer;
        IOResult result = pipe.reader.readExact(buffer);

        ASSERT_TRUE(result.error);
        ASSERT_EQ(result.error.value(), IOResult::Error::EndOfFile);
        ASSERT_EQ(result.bytesTransferred, 0);
    }
}

TEST_F(PipeTest, NonBlocking)
{
    Pipe pipe;

    constexpr std::array<uint8_t, 5> message = {'H', 'e', 'l', 'l', 'o'};

    // Set the reader to non-blocking mode
    pipe.reader.setNonBlocking();

    // Reading from an empty pipe returns WouldBlock
    {
        std::array<uint8_t, message.size()> buffer;
        IOResult result = pipe.reader.readExact(buffer);

        ASSERT_TRUE(result.error);
        ASSERT_EQ(result.error.value(), IOResult::Error::WouldBlock);
        ASSERT_EQ(result.bytesTransferred, 0);
    }

    // Write to the pipe
    {
        IOResult result = pipe.writer.writeAll({message});
        ASSERT_FALSE(result.error);
        ASSERT_EQ(result.bytesTransferred, message.size());
    }

    // Read from the pipe
    {
        std::array<uint8_t, message.size()> buffer;
        IOResult result = pipe.reader.readExact(buffer);

        ASSERT_FALSE(result.error);
        ASSERT_EQ(result.bytesTransferred, message.size());

        ASSERT_EQ(buffer, message);
    }

    // Attempt to read again from the pipe again returns WouldBlock
    {
        std::array<uint8_t, 1> buffer;
        IOResult result = pipe.reader.readExact(buffer);

        ASSERT_TRUE(result.error);
        ASSERT_EQ(result.error.value(), IOResult::Error::WouldBlock);
        ASSERT_EQ(result.bytesTransferred, 0);
    }
}

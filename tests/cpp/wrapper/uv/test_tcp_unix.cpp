#include <gtest/gtest.h>
#include <uv.h>

#include <array>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <thread>

#include "scaler/wrapper/uv/callback.h"
#include "scaler/wrapper/uv/error.h"
#include "scaler/wrapper/uv/loop.h"
#include "scaler/wrapper/uv/socket_address.h"
#include "scaler/wrapper/uv/tcp.h"

class UVTCPTestUnix: public ::testing::Test {};

TEST_F(UVTCPTestUnix, WriteAfterRemoteGracefulCloseFailsWithEPIPE)
{
    // MessageConnection reads any other connection error from a write as an abort that the next read hides.

    constexpr int maxWrites = 100;
    constexpr std::chrono::milliseconds resetWait {10};

    // A write that fails with EPIPE raises SIGPIPE, which the YMQ IO thread blocks too.
    class IgnoreSIGPIPE {
    public:
        IgnoreSIGPIPE() noexcept: _previousHandler(std::signal(SIGPIPE, SIG_IGN))
        {
        }
        ~IgnoreSIGPIPE() noexcept
        {
            std::signal(SIGPIPE, _previousHandler);
        }

    private:
        void (*_previousHandler)(int);
    } ignoreSIGPIPE;

    scaler::wrapper::uv::Loop loop = UV_EXIT_ON_ERROR(scaler::wrapper::uv::Loop::init());

    scaler::wrapper::uv::TCPServer server = UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPServer::init(loop));
    UV_EXIT_ON_ERROR(
        server.bind(UV_EXIT_ON_ERROR(scaler::wrapper::uv::SocketAddress::IPv4("127.0.0.1", 0)), uv_tcp_flags(0)));

    // The remote shuts down its side, which sends a FIN, then closes with nothing left unread.
    std::optional<scaler::wrapper::uv::TCPSocket> remote = UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPSocket::init(loop));
    bool remoteShutDown                                  = false;

    UV_EXIT_ON_ERROR(server.listen(16, [&](std::expected<void, scaler::wrapper::uv::Error> result) {
        UV_EXIT_ON_ERROR(result);
        UV_EXIT_ON_ERROR(server.accept(*remote));
        UV_EXIT_ON_ERROR(remote->shutdown([&](std::expected<void, scaler::wrapper::uv::Error> shutdownResult) {
            UV_EXIT_ON_ERROR(shutdownResult);
            remoteShutDown = true;
        }));
    }));

    scaler::wrapper::uv::TCPSocket client = UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPSocket::init(loop));
    bool clientReadEOF                    = false;

    UV_EXIT_ON_ERROR(client.connect(
        UV_EXIT_ON_ERROR(server.getSockName()), [&](std::expected<void, scaler::wrapper::uv::Error> result) {
            UV_EXIT_ON_ERROR(result);
            UV_EXIT_ON_ERROR(
                client.readStart([&](std::expected<std::span<const uint8_t>, scaler::wrapper::uv::Error> readResult) {
                    if (!readResult.has_value() && readResult.error().code() == UV_EOF) {
                        clientReadEOF = true;
                    }
                }));
        }));

    while (!remoteShutDown || !clientReadEOF) {
        loop.run(UV_RUN_ONCE);
    }

    remote.reset();

    // A write reaches the closed socket, so the remote resets the connection, and a later write fails.
    const std::array<uint8_t, 1> byte {'x'};
    std::optional<scaler::wrapper::uv::Error> writeError;

    for (int write_i = 0; write_i < maxWrites && !writeError.has_value(); ++write_i) {
        bool writeDone = false;

        auto result = client.write(
            std::span<const uint8_t>(byte), [&](std::expected<void, scaler::wrapper::uv::Error> writeResult) {
                writeDone = true;
                if (!writeResult.has_value()) {
                    writeError = writeResult.error();
                }
            });
        ASSERT_TRUE(result.has_value()) << result.error().name();

        while (!writeDone) {
            loop.run(UV_RUN_ONCE);
        }

        std::this_thread::sleep_for(resetWait);
    }

    ASSERT_TRUE(writeError.has_value()) << "no write failed after the remote closed";
    ASSERT_EQ(writeError->code(), UV_EPIPE) << writeError->name();
}

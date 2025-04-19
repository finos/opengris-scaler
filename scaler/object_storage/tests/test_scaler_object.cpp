#include <capnp/message.h>
#include <capnp/serialize.h>
#include <gtest/gtest.h>

#include <boost/asio.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/write.hpp>
#include <boost/asio/write_at.hpp>

#include "../constants.h"
#include "../defs.h"
#include "protocol/object_storage.capnp.h"

using boost::asio::buffer;
using boost::asio::ip::tcp;
using req_type  = ObjectRequestHeader::ObjectRequestType;
using resp_type = ObjectResponseHeader::ObjectResponseType;
using boost::asio::awaitable;
using scaler::object_storage::CAPNP_HEADER_SIZE;
using scaler::object_storage::CAPNP_WORD_SIZE;

char payload[] = "Hello, world!";

awaitable<void> async_read_response_header(tcp::socket& socket, scaler::object_storage::ObjectResponseHeader& header) {
    try {
        std::array<uint64_t, CAPNP_HEADER_SIZE / CAPNP_WORD_SIZE> buf;
        std::size_t n = co_await boost::asio::async_read(socket, boost::asio::buffer(buf.data(), CAPNP_HEADER_SIZE));

        capnp::FlatArrayMessageReader reader(
            kj::ArrayPtr<const capnp::word>((const capnp::word*)buf.data(), CAPNP_HEADER_SIZE / CAPNP_WORD_SIZE));
        auto request_root = reader.getRoot<::ObjectResponseHeader>();

        header.resp_type      = request_root.getResponseType();
        header.payload_length = request_root.getPayloadLength();

        auto object_id   = request_root.getObjectID();
        header.object_id = {
            object_id.getField0(),
            object_id.getField1(),
            object_id.getField2(),
            object_id.getField3(),
        };
    } catch (boost::system::system_error& e) {
        // TODO: make this a log, since eof is not really an err.
        printf("exception throwned, read error e.what() = %s\n", e.what());
        throw e;
    } catch (std::exception& e) {
        // TODO: make this a log, capnp header corruption is an err.
        printf("exception throwned, header not a capnp e.what() = %s\n", e.what());
        throw e;
    }
}

awaitable<void> async_write_request_header(
    boost::asio::ip::tcp::socket& socket,
    scaler::object_storage::ObjectRequestHeader& header,
    uint64_t payload_length) {
    capnp::MallocMessageBuilder return_msg;
    auto resp_root = return_msg.initRoot<::ObjectRequestHeader>();
    resp_root.setRequestType(header.req_type);
    resp_root.setPayloadLength(payload_length);
    auto resp_root_object_id = resp_root.initObjectID();
    resp_root_object_id.setField0(header.object_id[0]);
    resp_root_object_id.setField1(header.object_id[1]);
    resp_root_object_id.setField2(header.object_id[2]);
    resp_root_object_id.setField3(header.object_id[3]);

    auto buf = capnp::messageToFlatArray(return_msg);
    co_await boost::asio::async_write(socket, buffer(buf.asBytes().begin(), buf.asBytes().size()));
}

awaitable<void> testGetObjectByID(tcp::socket socket) {
    printf("testGetObjectByID\n");
    scaler::object_storage::ObjectRequestHeader request_header;
    request_header.req_type       = req_type::GET_OBJECT;
    request_header.object_id      = {1, 1, 2, 3};
    request_header.payload_length = sizeof(payload);
    co_await async_write_request_header(socket, request_header, request_header.payload_length);
    printf("testGetObjectByID reading response header\n");

    scaler::object_storage::ObjectResponseHeader response_header;
    co_await async_read_response_header(socket, response_header);
    EXPECT_EQ(response_header.payload_length, sizeof(payload));
    char buf[sizeof(payload)];
    co_await boost::asio::async_read(socket, buffer(buf));
    printf("testGetObjectByID finished\n");
}

awaitable<void> testSetObjectByID(tcp::socket socket) {
    // Just to make sure this function is invoked later than the other one
    sleep(1);
    printf("testSetObjectByID\n");
    scaler::object_storage::ObjectRequestHeader request_header;
    request_header.req_type       = req_type::SET_OBJECT;
    request_header.object_id      = {1, 1, 2, 3};
    request_header.payload_length = sizeof(payload);
    co_await async_write_request_header(socket, request_header, request_header.payload_length);
    co_await boost::asio::async_write(socket, buffer(payload));

    scaler::object_storage::ObjectResponseHeader response_header;
    co_await async_read_response_header(socket, response_header);

    printf("testSetObjectByID finished\n");
}

TEST(ObjectStorageTestSuite, TestHalt) {
    boost::asio::io_context io_context;
    tcp::socket socket(io_context);
    tcp::socket socket2(io_context);
    tcp::resolver resolver(io_context);
    boost::asio::connect(socket, resolver.resolve("127.0.0.1", "55555"));
    boost::asio::connect(socket2, resolver.resolve("127.0.0.1", "55555"));

    co_spawn(io_context.get_executor(), testGetObjectByID(std::move(socket)), boost::asio::detached);
    co_spawn(io_context.get_executor(), testSetObjectByID(std::move(socket2)), boost::asio::detached);

    io_context.run();

    boost::system::error_code ec;
    auto res = socket.shutdown(boost::asio::ip::tcp::socket::shutdown_both, ec);
    (void)res;
}

void write_request_header(
    boost::asio::ip::tcp::socket& socket,
    scaler::object_storage::ObjectRequestHeader& header,
    uint64_t payload_length) {
    capnp::MallocMessageBuilder return_msg;
    auto resp_root = return_msg.initRoot<::ObjectRequestHeader>();
    resp_root.setRequestType(header.req_type);
    resp_root.setPayloadLength(payload_length);
    auto resp_root_object_id = resp_root.initObjectID();
    resp_root_object_id.setField0(header.object_id[0]);
    resp_root_object_id.setField1(header.object_id[1]);
    resp_root_object_id.setField2(header.object_id[2]);
    resp_root_object_id.setField3(header.object_id[3]);

    auto buf = capnp::messageToFlatArray(return_msg);
    boost::asio::write(socket, buffer(buf.asBytes().begin(), buf.asBytes().size()));
}

void read_response_header(tcp::socket& socket, scaler::object_storage::ObjectResponseHeader& header) {
    try {
        std::array<uint64_t, CAPNP_HEADER_SIZE / CAPNP_WORD_SIZE> buf;
        boost::asio::read(socket, boost::asio::buffer(buf.data(), CAPNP_HEADER_SIZE));

        capnp::FlatArrayMessageReader reader(
            kj::ArrayPtr<const capnp::word>((const capnp::word*)buf.data(), CAPNP_HEADER_SIZE / CAPNP_WORD_SIZE));
        auto request_root = reader.getRoot<::ObjectResponseHeader>();

        header.resp_type      = request_root.getResponseType();
        header.payload_length = request_root.getPayloadLength();

        auto object_id   = request_root.getObjectID();
        header.object_id = {
            object_id.getField0(),
            object_id.getField1(),
            object_id.getField2(),
            object_id.getField3(),
        };
    } catch (boost::system::system_error& e) {
        // TODO: make this a log, since eof is not really an err.
        printf("exception throwned, read error e.what() = %s\n", e.what());
        throw e;
    } catch (std::exception& e) {
        // TODO: make this a log, capnp header corruption is an err.
        printf("exception throwned, header not a capnp e.what() = %s\n", e.what());
        throw e;
    }
}

void write_payload(boost::asio::ip::tcp::socket& socket) {
    boost::asio::write(socket, buffer(payload));
}

TEST(ObjectStorageTestSuite, TestSetObject) {
    boost::asio::io_context io_context;
    tcp::socket socket(io_context);
    tcp::resolver resolver(io_context);
    boost::asio::connect(socket, resolver.resolve("127.0.0.1", "55555"));

    scaler::object_storage::ObjectRequestHeader requestHeader;
    requestHeader.object_id      = {0, 1, 2, 3};
    requestHeader.payload_length = sizeof(payload);
    requestHeader.req_type       = req_type::SET_OBJECT;

    write_request_header(socket, requestHeader, sizeof(payload));
    write_payload(socket);

    scaler::object_storage::ObjectResponseHeader responseHeader;
    read_response_header(socket, responseHeader);

    EXPECT_EQ(responseHeader.object_id, requestHeader.object_id);
    EXPECT_EQ(responseHeader.payload_length, 0);
    EXPECT_EQ(responseHeader.resp_type, resp_type::SET_O_K);

    write_request_header(socket, requestHeader, sizeof(payload));
    write_payload(socket);
    read_response_header(socket, responseHeader);
    EXPECT_EQ(responseHeader.object_id, requestHeader.object_id);
    EXPECT_EQ(responseHeader.payload_length, 0);
    EXPECT_EQ(responseHeader.resp_type, resp_type::SET_O_K_OVERRIDE);

    boost::system::error_code ec;
    auto res = socket.shutdown(boost::asio::ip::tcp::socket::shutdown_both, ec);
    (void)res;
}

TEST(ObjectStorageTestSuite, TestGetObject) {
    boost::asio::io_context io_context;
    tcp::socket socket(io_context);
    tcp::resolver resolver(io_context);
    boost::asio::connect(socket, resolver.resolve("127.0.0.1", "55555"));

    char response_payload[sizeof(payload)];

    scaler::object_storage::ObjectRequestHeader requestHeader;
    requestHeader.object_id      = {0, 1, 2, 3};
    requestHeader.payload_length = sizeof(payload);
    requestHeader.req_type       = req_type::GET_OBJECT;

    write_request_header(socket, requestHeader, sizeof(payload));

    scaler::object_storage::ObjectResponseHeader responseHeader;
    read_response_header(socket, responseHeader);
    boost::asio::read(socket, buffer(response_payload));

    EXPECT_EQ(responseHeader.object_id, requestHeader.object_id);
    EXPECT_EQ(responseHeader.payload_length, sizeof(payload));
    EXPECT_EQ(responseHeader.resp_type, resp_type::GET_O_K);
    EXPECT_EQ(std::string(response_payload), std::string(payload));

    requestHeader.req_type = req_type::GET_OBJECT_HEADER;
    // NOTE: Get the first byte
    write_request_header(socket, requestHeader, 1);
    read_response_header(socket, responseHeader);
    EXPECT_EQ(responseHeader.object_id, requestHeader.object_id);
    EXPECT_EQ(responseHeader.payload_length, 1);
    EXPECT_EQ(responseHeader.resp_type, resp_type::GET_O_K);

    response_payload[0] = 'A';  // Just to make sure it's not 'H'
    boost::asio::read(socket, buffer(response_payload, 1));
    EXPECT_EQ(response_payload[0], payload[0]);

    boost::system::error_code ec;
    auto res = socket.shutdown(boost::asio::ip::tcp::socket::shutdown_both, ec);
    (void)res;
}

TEST(ObjectStorageTestSuite, TestDeleteObject) {
    boost::asio::io_context io_context;
    tcp::socket socket(io_context);
    tcp::resolver resolver(io_context);
    boost::asio::connect(socket, resolver.resolve("127.0.0.1", "55555"));

    scaler::object_storage::ObjectRequestHeader requestHeader;
    requestHeader.object_id      = {0, 1, 2, 3};
    requestHeader.payload_length = sizeof(payload);
    requestHeader.req_type       = req_type::DELETE_OBJECT;

    write_request_header(socket, requestHeader, sizeof(payload));

    scaler::object_storage::ObjectResponseHeader responseHeader;
    read_response_header(socket, responseHeader);

    EXPECT_EQ(responseHeader.object_id, requestHeader.object_id);
    EXPECT_EQ(responseHeader.payload_length, 0);
    EXPECT_EQ(responseHeader.resp_type, resp_type::DEL_O_K);

    write_request_header(socket, requestHeader, 0);
    read_response_header(socket, responseHeader);
    EXPECT_EQ(responseHeader.object_id, requestHeader.object_id);
    EXPECT_EQ(responseHeader.payload_length, 0);
    EXPECT_EQ(responseHeader.resp_type, resp_type::DEL_NOT_EXISTS);

    boost::system::error_code ec;
    auto res = socket.shutdown(boost::asio::ip::tcp::socket::shutdown_both, ec);
    (void)res;
}

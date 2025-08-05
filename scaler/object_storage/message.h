#pragma once

#include <capnp/serialize.h>

#include <array>
#include <vector>

#include "protocol/object_storage.capnp.h"

namespace scaler {
namespace object_storage {

static constexpr size_t CAPNP_HEADER_SIZE = 80;
static constexpr size_t CAPNP_WORD_SIZE   = sizeof(capnp::word);

template <typename T>
concept ObjectStorageMessage = requires(const T obj, std::vector<capnp::word> buffer) {
    { T::bufferSize() } -> std::same_as<size_t>;

    { obj.toBuffer() } -> std::same_as<kj::Array<const capnp::word>>;

    { T::fromBuffer(buffer) } -> std::same_as<T>;
};

struct ObjectID {
    std::array<uint64_t, 4> value;

    constexpr ObjectID() {};

    constexpr ObjectID(uint64_t v0, uint64_t v1, uint64_t v2, uint64_t v3): value({v0, v1, v2, v3}) {};

    constexpr uint64_t& operator[](size_t index) { return value[index]; }

    constexpr const uint64_t& operator[](size_t index) const { return value[index]; }

    constexpr std::strong_ordering operator<=>(const ObjectID& other) const = default;

    static constexpr size_t bufferSize() { return 48; }

    kj::Array<const capnp::word> toBuffer() const;

    template <typename Buffer>
    static ObjectID fromBuffer(const Buffer& buffer)
    {
        capnp::FlatArrayMessageReader reader(
            kj::ArrayPtr<const capnp::word>((const capnp::word*)buffer.data(), bufferSize() / CAPNP_WORD_SIZE));

        auto objectIDRoot = reader.getRoot<scaler::protocol::ObjectID>();

        return {objectIDRoot.getField0(), objectIDRoot.getField1(), objectIDRoot.getField2(), objectIDRoot.getField3()};
    }
};

static_assert(ObjectStorageMessage<ObjectID>);

struct ObjectRequestHeader {
    ObjectID objectID;
    uint64_t payloadLength;
    uint64_t requestID;
    scaler::protocol::ObjectRequestHeader::ObjectRequestType requestType;

    static constexpr size_t bufferSize() { return CAPNP_HEADER_SIZE; }

    kj::Array<const capnp::word> toBuffer() const;

    template <typename Buffer>
    static ObjectRequestHeader fromBuffer(const Buffer& buffer)
    {
        capnp::FlatArrayMessageReader reader(
            kj::ArrayPtr<const capnp::word>((const capnp::word*)buffer.data(), bufferSize() / CAPNP_WORD_SIZE));

        auto requestRoot  = reader.getRoot<scaler::protocol::ObjectRequestHeader>();
        auto objectIDRoot = requestRoot.getObjectID();

        return ObjectRequestHeader {
            .objectID =
                {
                    objectIDRoot.getField0(),
                    objectIDRoot.getField1(),
                    objectIDRoot.getField2(),
                    objectIDRoot.getField3(),
                },
            .payloadLength = requestRoot.getPayloadLength(),
            .requestID     = requestRoot.getRequestID(),
            .requestType   = requestRoot.getRequestType(),
        };
    }
};

static_assert(ObjectStorageMessage<ObjectRequestHeader>);

struct ObjectResponseHeader {
    ObjectID objectID;
    uint64_t payloadLength;
    uint64_t responseID;
    scaler::protocol::ObjectResponseHeader::ObjectResponseType responseType;

    static constexpr size_t bufferSize() { return CAPNP_HEADER_SIZE; }

    kj::Array<const capnp::word> toBuffer() const;

    template <typename Buffer>
    static ObjectResponseHeader fromBuffer(const Buffer& buffer)
    {
        capnp::FlatArrayMessageReader reader(
            kj::ArrayPtr<const capnp::word>((const capnp::word*)buffer.data(), bufferSize() / CAPNP_WORD_SIZE));

        auto responseRoot = reader.getRoot<scaler::protocol::ObjectResponseHeader>();
        auto objectIDRoot = responseRoot.getObjectID();

        return ObjectResponseHeader {
            .objectID =
                {
                    objectIDRoot.getField0(),
                    objectIDRoot.getField1(),
                    objectIDRoot.getField2(),
                    objectIDRoot.getField3(),
                },
            .payloadLength = responseRoot.getPayloadLength(),
            .responseID    = responseRoot.getResponseID(),
            .responseType  = responseRoot.getResponseType(),
        };
    }
};

static_assert(ObjectStorageMessage<ObjectResponseHeader>);

};  // namespace object_storage
};  // namespace scaler

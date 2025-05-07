#pragma once

#include <array>
#include <boost/asio/ip/tcp.hpp>
#include <memory>
#include <vector>

#include "protocol/object_storage.capnp.h"

namespace scaler {
namespace object_storage {

using object_id_t     = std::array<uint64_t, 4>;
using object_t        = std::vector<unsigned char>;
using shared_object_t = std::shared_ptr<object_t>;
using payload_t       = std::vector<unsigned char>;

struct ObjectRequestHeader {
    object_id_t objectID;
    uint64_t payloadLength;
    uint64_t requestID;
    ::ObjectRequestHeader::ObjectRequestType reqType;
    ObjectRequestHeader(): objectID {}, payloadLength {}, requestID {}, reqType {} {}
};

struct ObjectResponseHeader {
    object_id_t objectID;
    uint64_t payloadLength;
    uint64_t responseID;
    ::ObjectResponseHeader::ObjectResponseType respType;
    ObjectResponseHeader(): objectID {}, payloadLength {}, responseID {}, respType {} {}
};

};  // namespace object_storage
};  // namespace scaler

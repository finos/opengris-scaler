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
    object_id_t object_id;
    uint64_t payload_length;
    ::ObjectRequestHeader::ObjectRequestType req_type;
    ObjectRequestHeader(): object_id {}, payload_length {}, req_type {} {}
};

struct ObjectResponseHeader {
    object_id_t object_id;
    uint64_t payload_length;
    ::ObjectResponseHeader::ObjectResponseType resp_type;
    ObjectResponseHeader(): object_id {}, payload_length {}, resp_type {} {}
};

};  // namespace object_storage
};  // namespace scaler

#include "scaler/object_storage/object_manager.h"

#include <algorithm>
#include <cassert>
#include <string_view>

namespace scaler {
namespace object_storage {

static std::size_t computePayloadHash(const scaler::ymq::Bytes& payload) noexcept
{
    return std::hash<std::string_view> {}({reinterpret_cast<const char*>(payload.data()), payload.size()});
}

static bool hasSameBytes(const ObjectPayload& left, const ObjectPayload& right) noexcept
{
    return std::equal(left.data(), left.data() + left.size(), right.data(), right.data() + right.size());
}

ObjectManager::ObjectManager(): ObjectManager(computePayloadHash)
{
}

ObjectManager::ObjectManager(PayloadHashFunction payloadHashFunction)
    : totalObjectsBytes {}, uniqueObjectsCount {}, payloadHashFunction {payloadHashFunction}
{
}

std::shared_ptr<const ObjectPayload> ObjectManager::setObject(
    const ObjectID& objectID, std::unique_ptr<ObjectPayload> payload) noexcept
{
    if (hasObject(objectID)) {
        // Overriding object: delete old first
        deleteObject(objectID);
    }

    const ObjectHash hash = payloadHashFunction(*payload);

    std::vector<ManagedObject>& bucket = hashToObjects[hash];

    auto objectIt = std::ranges::find_if(
        bucket, [&payload](const ManagedObject& object) { return hasSameBytes(*object.payload, *payload); });

    if (objectIt == bucket.end()) {
        // New object payload
        objectIt = bucket.insert(
            bucket.end(),
            ManagedObject {
                .useCount = 1,
                // Converts unique_ptr -> shared_ptr, keeping the Bytes subtype intact.
                // Two allocations (object + separate control block); use
                // make_shared<BufferedBytes> if this ever becomes a hot path.
                .payload = std::shared_ptr<const ObjectPayload>(std::move(payload)),
            });
        totalObjectsBytes += objectIt->payload->size();
        ++uniqueObjectsCount;
    } else {
        // Known object payload
        ++(objectIt->useCount);
    }

    objectIDToObject[objectID] = ObjectLocation {.hash = hash, .payload = objectIt->payload};

    return objectIt->payload;
}

std::shared_ptr<const ObjectPayload> ObjectManager::getObject(const ObjectID& objectID) const noexcept
{
    auto locationIt = objectIDToObject.find(objectID);

    if (locationIt == objectIDToObject.end()) {
        return SharedObjectPayload(nullptr);
    }

    return locationIt->second.payload;
}

bool ObjectManager::deleteObject(const ObjectID& objectID) noexcept
{
    auto locationIt = objectIDToObject.find(objectID);

    if (locationIt == objectIDToObject.end()) {
        return false;
    }

    const ObjectLocation& location = locationIt->second;

    auto bucketIt                      = hashToObjects.find(location.hash);
    std::vector<ManagedObject>& bucket = bucketIt->second;

    auto objectIt = std::ranges::find(bucket, location.payload, &ManagedObject::payload);

    --objectIt->useCount;
    if (objectIt->useCount < 1) {
        assert(totalObjectsBytes >= objectIt->payload->size());
        totalObjectsBytes -= objectIt->payload->size();
        --uniqueObjectsCount;

        bucket.erase(objectIt);
        if (bucket.empty()) {
            hashToObjects.erase(bucketIt);
        }
    }

    objectIDToObject.erase(locationIt);

    return true;
}

std::shared_ptr<const ObjectPayload> ObjectManager::duplicateObject(
    const ObjectID& originalObjectID, const ObjectID& newObjectID) noexcept
{
    auto locationIt = objectIDToObject.find(originalObjectID);

    if (locationIt == objectIDToObject.end()) {
        return nullptr;
    }

    if (originalObjectID == newObjectID) {
        // Overriding the object with itself would delete it first.
        return locationIt->second.payload;
    }

    if (hasObject(newObjectID)) {
        // Overriding object: delete old first
        deleteObject(newObjectID);
    }

    const ObjectLocation location = locationIt->second;

    std::vector<ManagedObject>& bucket = hashToObjects[location.hash];

    auto objectIt = std::ranges::find(bucket, location.payload, &ManagedObject::payload);

    ++(objectIt->useCount);

    objectIDToObject[newObjectID] = location;

    return location.payload;
}

bool ObjectManager::hasObject(const ObjectID& objectID) const noexcept
{
    return objectIDToObject.contains(objectID);
}

size_t ObjectManager::size() const noexcept
{
    return objectIDToObject.size();
}

size_t ObjectManager::sizeUnique() const noexcept
{
    return uniqueObjectsCount;
}

};  // namespace object_storage
};  // namespace scaler

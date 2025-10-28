#include "scaler/object_storage/object_manager.h"

#include <algorithm>
#include <cassert>

template <>
struct std::hash<scaler::object_storage::ObjectPayload> {
    std::size_t operator()(const scaler::object_storage::ObjectPayload& payload) const noexcept
    {
        return std::hash<std::string_view> {}({reinterpret_cast<const char*>(payload.data()), payload.size()});
    }
};

namespace scaler {
namespace object_storage {

ObjectManager::ObjectManager(): totalObjectsBytes {}
{
}

std::shared_ptr<const ObjectPayload> ObjectManager::setObject(
    const ObjectID& objectID, ObjectPayload&& payload) noexcept
{
    if (hasObject(objectID)) {
        // Overriding object: delete old first
        deleteObject(objectID);
    }

    ObjectHash hash = std::hash<ObjectPayload> {}(payload);

    objectIDToHash[objectID] = hash;

    auto objectIt = hashToObject.find(hash);

    if (objectIt == hashToObject.end()) {
        // New object payload
        objectIt = hashToObject
                       .emplace(
                           hash,
                           ManagedObject {
                               .useCount = 1,
                               .payload  = std::make_shared<const ObjectPayload>(std::move(payload)),
                           })
                       .first;
        totalObjectsBytes += objectIt->second.payload->size();
    } else {
        // Known object payload
        ++(objectIt->second.useCount);
    }

    return objectIt->second.payload;
}

std::shared_ptr<const ObjectPayload> ObjectManager::getObject(const ObjectID& objectID) const noexcept
{
    auto hashIt = objectIDToHash.find(objectID);

    if (hashIt == objectIDToHash.end()) {
        return SharedObjectPayload(nullptr);
    }

    return hashToObject.at(hashIt->second).payload;
}

bool ObjectManager::deleteObject(const ObjectID& objectID) noexcept
{
    auto hashIt = objectIDToHash.find(objectID);

    if (hashIt == objectIDToHash.end()) {
        return false;
    }

    const ObjectHash& hash = hashIt->second;

    auto objectIt = hashToObject.find(hash);

    --objectIt->second.useCount;
    if (objectIt->second.useCount < 1) {
        assert(totalObjectsBytes >= objectIt->second.payload->size());
        totalObjectsBytes -= objectIt->second.payload->size();

        hashToObject.erase(objectIt);
    }

    objectIDToHash.erase(hashIt);

    return true;
}

std::shared_ptr<const ObjectPayload> ObjectManager::duplicateObject(
    const ObjectID& originalObjectID, const ObjectID& newObjectID) noexcept
{
    auto hashIt = objectIDToHash.find(originalObjectID);

    if (hashIt == objectIDToHash.end()) {
        return nullptr;
    }

    if (hasObject(newObjectID)) {
        // Overriding object: delete old first
        deleteObject(newObjectID);
    }

    auto hash = hashIt->second;

    ManagedObject& object = hashToObject[hash];

    ++(object.useCount);

    objectIDToHash[newObjectID] = hash;

    return object.payload;
}

bool ObjectManager::hasObject(const ObjectID& objectID) const noexcept
{
    return objectIDToHash.contains(objectID);
}

size_t ObjectManager::size() const noexcept
{
    return objectIDToHash.size();
}

size_t ObjectManager::sizeUnique() const noexcept
{
    return hashToObject.size();
}

};  // namespace object_storage
};  // namespace scaler

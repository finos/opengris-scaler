#pragma once

#include <map>
#include <memory>

#include "scaler/object_storage/defs.h"
#include "scaler/object_storage/message.h"

namespace scaler {
namespace object_storage {

class ObjectManager {
public:
    ObjectManager();

    // Returns the pointer to the created (and moved) object.
    std::shared_ptr<const ObjectPayload> setObject(const ObjectID& objectID, ObjectPayload&& payload) noexcept;

    // Returns `nullptr` if the object does not exist.
    std::shared_ptr<const ObjectPayload> getObject(const ObjectID& objectID) const noexcept;

    // Returns `true` if the deleted object existed, otherwise returns `false`.
    bool deleteObject(const ObjectID& objectID) noexcept;

    // Creates a new `ObjectID` referencing the same object's content as `originalObjectID`. Overrides `newObjectID` if
    // it already exist.
    // Returns `nullptr` if `originalObjectID` does not exist, otherwise returns the object's content.
    std::shared_ptr<const ObjectPayload> duplicateObject(
        const ObjectID& originalObjectID, const ObjectID& newObjectID) noexcept;

    bool hasObject(const ObjectID& objectID) const noexcept;

    // Returns the total number of objects stored.
    size_t size() const noexcept;

    // Returns the total number of unique objects stored (i.e. only count duplicate payloads once).
    size_t sizeUnique() const noexcept;

    size_t totalObjectsSize() const noexcept { return totalObjectsBytes; };

private:
    using ObjectHash = std::size_t;

    struct ManagedObject {
        size_t useCount;
        std::shared_ptr<const ObjectPayload> payload;
    };

    std::map<ObjectID, ObjectHash> objectIDToHash;
    std::map<ObjectHash, ManagedObject> hashToObject;
    size_t totalObjectsBytes;
};

};  // namespace object_storage
};  // namespace scaler

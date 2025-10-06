#include <gtest/gtest.h>

#include "scaler/object_storage/defs.h"
#include "scaler/object_storage/object_manager.h"

const scaler::object_storage::ObjectPayload payload {std::string("Hello")};

TEST(ObjectManagerTestSuite, TestSetObject)
{
    scaler::object_storage::ObjectManager objectManager;

    scaler::object_storage::ObjectID objectID1 {0, 1, 2, 3};

    EXPECT_FALSE(objectManager.hasObject(objectID1));
    EXPECT_EQ(objectManager.size(), 0);
    EXPECT_EQ(objectManager.sizeUnique(), 0);

    objectManager.setObject(objectID1, scaler::object_storage::ObjectPayload(payload));

    EXPECT_TRUE(objectManager.hasObject(objectID1));
    EXPECT_EQ(objectManager.size(), 1);
    EXPECT_EQ(objectManager.sizeUnique(), 1);

    scaler::object_storage::ObjectID objectID2 {3, 2, 1, 0};

    objectManager.setObject(objectID2, scaler::object_storage::ObjectPayload(payload));

    EXPECT_TRUE(objectManager.hasObject(objectID2));
    EXPECT_EQ(objectManager.size(), 2);
    EXPECT_EQ(objectManager.sizeUnique(), 1);
}

TEST(ObjectManagerTestSuite, TestGetObject)
{
    scaler::object_storage::ObjectManager objectManager;

    scaler::object_storage::ObjectID objectID1 {0, 1, 2, 3};

    auto payloadPtr = objectManager.getObject(objectID1);

    EXPECT_EQ(payloadPtr, nullptr);  // not yet existing object

    objectManager.setObject(objectID1, scaler::object_storage::ObjectPayload(payload));

    payloadPtr = objectManager.getObject(objectID1);

    EXPECT_EQ(*payloadPtr, payload);
}

TEST(ObjectManagerTestSuite, TestDeleteObject)
{
    scaler::object_storage::ObjectManager objectManager;

    scaler::object_storage::ObjectID objectID1 {0, 1, 2, 3};

    objectManager.setObject(objectID1, scaler::object_storage::ObjectPayload(payload));

    bool deleted = objectManager.deleteObject(objectID1);
    EXPECT_TRUE(deleted);

    EXPECT_FALSE(objectManager.hasObject(objectID1));
    EXPECT_EQ(objectManager.size(), 0);
    EXPECT_EQ(objectManager.sizeUnique(), 0);

    deleted = objectManager.deleteObject(objectID1);  // deleting unknown object
    EXPECT_FALSE(deleted);
}

TEST(ObjectManagerTestSuite, TestDuplicateObject)
{
    scaler::object_storage::ObjectManager objectManager;

    scaler::object_storage::ObjectID objectID1 {0, 1, 2, 3};
    scaler::object_storage::ObjectID objectID2 {0, 1, 2, 4};

    // Cannot duplicate a non existing object.
    auto duplicatedObject = objectManager.duplicateObject(objectID1, objectID2);
    EXPECT_EQ(duplicatedObject, nullptr);

    objectManager.setObject(objectID1, scaler::object_storage::ObjectPayload(payload));

    duplicatedObject = objectManager.duplicateObject(objectID1, objectID2);
    EXPECT_NE(duplicatedObject, nullptr);
    EXPECT_EQ(*duplicatedObject, payload);

    // Deleting the first object does not remove the duplicated one.
    objectManager.deleteObject(objectID1);
    EXPECT_TRUE(objectManager.hasObject(objectID2));
    EXPECT_EQ(objectManager.size(), 1);
    EXPECT_EQ(objectManager.sizeUnique(), 1);
}

TEST(ObjectManagerTestSuite, TestReferenceCountObject)
{
    scaler::object_storage::ObjectManager objectManager;

    scaler::object_storage::ObjectID objectID1 {11, 0, 0, 0};
    objectManager.setObject(objectID1, scaler::object_storage::ObjectPayload(payload));

    scaler::object_storage::ObjectID objectID2 {12, 0, 0, 0};
    objectManager.setObject(objectID2, scaler::object_storage::ObjectPayload(payload));

    EXPECT_EQ(objectManager.size(), 2);
    EXPECT_EQ(objectManager.sizeUnique(), 1);

    auto payloadPtr1 = objectManager.getObject(objectID1);
    auto payloadPtr2 = objectManager.getObject(objectID2);

    EXPECT_EQ(payloadPtr1, payloadPtr2);  // should use the same memory location

    objectManager.deleteObject(objectID1);

    EXPECT_EQ(objectManager.size(), 1);
    EXPECT_EQ(objectManager.sizeUnique(), 1);

    objectManager.deleteObject(objectID2);

    EXPECT_EQ(objectManager.size(), 0);
    EXPECT_EQ(objectManager.sizeUnique(), 0);
}

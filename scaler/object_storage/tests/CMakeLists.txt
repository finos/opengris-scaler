
include(FindGTest)

add_executable(
  test_scaler_object
  test_scaler_object.cpp
  ../io_helper.cpp
)

target_link_libraries(
  test_scaler_object PRIVATE
  GTest::gtest_main
)

target_sources(test_scaler_object PRIVATE ${objectStorageSources})
target_link_libraries(test_scaler_object PRIVATE CapnProto::capnp)
target_include_directories(test_scaler_object PRIVATE ${CMAKE_BINARY_DIR})


include(GoogleTest)
gtest_discover_tests(test_scaler_object)



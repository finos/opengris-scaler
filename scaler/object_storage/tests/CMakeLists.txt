include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/155b337c938a2953e5675f9dc18c99f05f4c85d0.zip
)
# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

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

# add_executable(test_scaler_so test_scaler_so.cpp)
# target_include_directories(test_scaler_so PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/..)
# target_link_libraries(test_scaler_so PRIVATE ${CMAKE_CURRENT_BINARY_DIR}/../libserver_as_so.so)

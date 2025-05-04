include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/155b337c938a2953e5675f9dc18c99f05f4c85d0.zip
)
# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
set(BUILD_GMOCK OFF CACHE BOOL "" FORCE)
set(BUILD_GTEST ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

add_executable(
  test_update_record
  test_update_record.cpp
  ${PROJECT_SOURCE_DIR}/scaler/object_storage/io_helper.cpp
)

target_include_directories(test_update_record
  PRIVATE ${CMAKE_BINARY_DIR}
)

target_link_libraries(
  test_update_record 
  PRIVATE GTest::gtest_main
  PRIVATE CapnProto::capnp
)

add_test(NAME test_update_record COMMAND test_update_record)

add_executable(
  test_scaler_object
  test_scaler_object.cpp
  ${PROJECT_SOURCE_DIR}/scaler/object_storage/io_helper.cpp
  ${PROJECT_SOURCE_DIR}/scaler/object_storage/server.cpp
  ${CMAKE_BINARY_DIR}/protocol/object_storage.capnp.c++
)

target_include_directories(test_scaler_object 
  PRIVATE ${CMAKE_BINARY_DIR}
)

target_link_libraries(
  test_scaler_object 
  PRIVATE GTest::gtest_main
  PRIVATE CapnProto::capnp
)

add_test(NAME test_scaler_object COMMAND test_scaler_object)

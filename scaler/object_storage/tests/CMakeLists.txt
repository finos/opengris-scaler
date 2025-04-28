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

include(FetchContent)
set(ARGPARSE_GIT_REPOSITORY "https://github.com/p-ranav/argparse.git")
set(ARGPARSE_GIT_TAG "d924b84eba1f0f0adf38b20b7b4829f6f65b6570")
FetchContent_Declare(
    argparse
    GIT_REPOSITORY ${ARGPARSE_GIT_REPOSITORY}
    GIT_TAG ${ARGPARSE_GIT_TAG}
)
FetchContent_MakeAvailable(argparse)
message(STATUS "Fetched argparse from ${ARGPARSE_GIT_REPOSITORY} @ ${ARGPARSE_GIT_TAG}")

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

add_executable(test_server_main 
  test_server_main.cpp
  ../io_helper.cpp
)

target_include_directories(test_server_main PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/..)
target_sources(test_server_main PRIVATE ${objectStorageSources})
target_link_libraries(test_server_main PRIVATE CapnProto::capnp)
target_include_directories(test_server_main PRIVATE ${CMAKE_BINARY_DIR})
target_link_libraries(test_server_main PRIVATE argparse)

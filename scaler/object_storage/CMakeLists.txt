
find_package(Boost CONFIG REQUIRED)
message(STATUS "Found Boost in ${Boost_INCLUDE_DIRS}")

include(FetchContent)
FetchContent_Declare(
    argparse
    GIT_REPOSITORY https://github.com/p-ranav/argparse.git
)
FetchContent_MakeAvailable(argparse)
message(STATUS "Fetched argparse from https://github.com/p-ranav/argparse.git")


include_directories(${Boost_INCLUDE_DIRS})

add_executable(server)
target_sources(server PRIVATE
  server.cpp
  io_helper.cpp
)

target_link_libraries(server PRIVATE argparse)

set(CAPNPC_SRC_PREFIX "${PROJECT_SOURCE_DIR}/scaler/protocol/capnp" CACHE STRING "" FORCE)

set(CAPNPC_OUTPUT_DIR "${CMAKE_BINARY_DIR}/protocol" CACHE STRING "" FORCE)

file(MAKE_DIRECTORY "${CAPNPC_OUTPUT_DIR}")

capnp_generate_cpp(objectStorageSources objectStorageHeaders "../protocol/capnp/object_storage.capnp")

target_sources(server PRIVATE ${objectStorageSources})
target_link_libraries(server PRIVATE CapnProto::capnp)
target_include_directories(server PRIVATE ${CMAKE_BINARY_DIR})

add_subdirectory(tests)


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

add_executable(server)
# add_library(server_as_so SHARED)

target_sources(server PRIVATE
  server.cpp
  io_helper.cpp
)

# target_sources(server_as_so PRIVATE
#   server_as_so.cpp
#   io_helper.cpp
# )

target_link_libraries(server PRIVATE argparse)
# target_link_libraries(server_as_so PRIVATE argparse)

set(CAPNPC_SRC_PREFIX "${PROJECT_SOURCE_DIR}/scaler/protocol/capnp" CACHE STRING "" FORCE)
set(CAPNPC_OUTPUT_DIR "${CMAKE_BINARY_DIR}/protocol" CACHE STRING "" FORCE)

file(MAKE_DIRECTORY "${CAPNPC_OUTPUT_DIR}")

capnp_generate_cpp(objectStorageSources objectStorageHeaders "../protocol/capnp/object_storage.capnp")

target_sources(server PRIVATE ${objectStorageSources})
target_link_libraries(server PRIVATE CapnProto::capnp)
target_include_directories(server PRIVATE ${CMAKE_BINARY_DIR})

# target_sources(server_as_so PRIVATE ${objectStorageSources})
# target_link_libraries(server_as_so PRIVATE CapnProto::capnp)
# target_include_directories(server_as_so PRIVATE ${CMAKE_BINARY_DIR})

add_subdirectory(tests)

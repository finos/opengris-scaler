set(BUILD_SHARED_LIBS ON CACHE BOOL "Build shared libraries")

find_package(CapnProto CONFIG REQUIRED)

get_target_property(CAPNP_INCLUDE_DIRS CapnProto::capnp INTERFACE_INCLUDE_DIRECTORIES)
message(STATUS "Found Capnp in ${CAPNP_INCLUDE_DIRS}")
# Make LSP happy
include_directories(${CAPNP_INCLUDE_DIRS})

add_subdirectory(object_storage)


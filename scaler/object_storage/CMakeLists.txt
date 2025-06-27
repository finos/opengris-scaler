add_library(cc_object_storage_server SHARED)

target_sources(cc_object_storage_server PRIVATE
  object_storage_server.h
  io_helper.cpp
)

set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/scaler/object_storage)

set(CAPNPC_SRC_PREFIX "${PROJECT_SOURCE_DIR}/scaler/protocol/capnp" CACHE STRING "" FORCE)
set(CAPNPC_OUTPUT_DIR "${CMAKE_BINARY_DIR}/protocol" CACHE STRING "" FORCE)

file(MAKE_DIRECTORY "${CAPNPC_OUTPUT_DIR}")

capnp_generate_cpp(objectStorageSources objectStorageHeaders "../protocol/capnp/object_storage.capnp")

target_sources(cc_object_storage_server PUBLIC ${objectStorageSources})
target_link_libraries(cc_object_storage_server PUBLIC CapnProto::capnp)
target_include_directories(cc_object_storage_server PUBLIC ${CMAKE_BINARY_DIR})

install(TARGETS cc_object_storage_server
        LIBRARY DESTINATION scaler/object_storage/)

find_package(Python3 COMPONENTS Development.Module REQUIRED)

add_library(object_storage_server SHARED)

set_target_properties(object_storage_server PROPERTIES  PREFIX "")

target_sources(object_storage_server PRIVATE pymod_object_storage_server.cpp)
target_include_directories(object_storage_server PRIVATE ${Python3_INCLUDE_DIRS})
target_link_libraries(object_storage_server PRIVATE cc_object_storage_server
                                            PRIVATE ${Python3_LIBRARIES}
)

target_link_options(object_storage_server PRIVATE "-Wl,-rpath,$ORIGIN")

install(TARGETS object_storage_server
        LIBRARY DESTINATION scaler/object_storage/)
